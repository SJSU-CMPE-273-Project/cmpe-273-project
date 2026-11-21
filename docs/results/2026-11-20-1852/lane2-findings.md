# Lane 2 end-to-end findings (PRs #34–#37), 2026-11-20

Tested `lane2/dlq` (#37, which contains #34–#36) merged with the Lane 3 harness, full path:
load generator → Iggy → ingest bridge → Kafka → delivery worker → Toxiproxy → SMS adapter → oracle.
Commit `9d04215` in the outputs is a local, unpushed merge of `lane2/dlq` @ `079c8bc` into `lane3/load-chaos-harness` @ `9f0d2d2`, plus the harness changes listed at the end of this file (committed to #33 right after this run).
Raw output: [summary.md](summary.md) and `T*.txt` in this folder. The first run with the old grading is in `../2026-11-20-1839/`.

## Results

| Test | Result | Key numbers |
|---|---|---|
| T0 baseline (10/s, 30 s) | PASS | 300/300, 0 dup, order OK, p99 1.45 s |
| T1 SMS primary down 5 min | PASS | 0 lost, 0 dup, drain 43.8 s after restart (SLO ≤ 120 s). See finding 3 |
| T2 SIGKILL worker | PASS | 1200/1200, 0 dup, 0 lost. p50 35.6 s. See finding 4 |
| T4 fail N twice | PASS | delivered 1 2 3 in order |
| T5 poison → DLQ → replay | FAIL | DLQ + single replay correct; second replay not refused. Finding 2 |
| T6 primary provider outage | FAIL | **1 duplicate SMS.** Failover 0.39 s, p99 1.5 s. Finding 1 |

The happy path, ordering, idempotency on replays and crash recovery all hold. Great work.

## 1. Duplicate SMS during T6 failover (bug, worth fixing before Nov 30)

Event `d647b83f-…` (account `lg-eb88ab-0050`, seq 6) was delivered by **both** sms_primary and sms_secondary.

- Worker ledger: attempt 1 sms_primary `unavailable` (7 ms) at 03:00:52.474, attempt 2 `circuit_open`, attempt 3 sms_secondary ok.
- SMS primary adapter log: `send.accepted`, 200, at 03:00:52.481. It received the request **once**.
- Toxiproxy cut sms_primary at 03:00:52.

`unavailable` means the worker saw a `dial` error, which `adapter.go` treats as "request never sent, safe to fail over". But the adapter did get and accept it. Likely cause: Go's `http.Transport` automatically retries a request on a new connection when a **reused keep-alive connection** fails before the response, and it treats a POST as safe to retry when it has an `Idempotency-Key` header and a rewindable body (`http.NewRequest` with a `bytes.Reader` sets `GetBody`). So:

1. The first try goes out on the pooled connection → adapter accepts → Toxiproxy cuts → the response is lost.
2. The transport silently retries → the new dial fails (proxy disabled) → `Do` returns only the **dial** error.
3. The worker classifies it as definitive → fails over → second delivery.

Possible fix (one line, in `Adapters.Send` after building the request): `req.GetBody = nil`. Without `GetBody` the transport can't replay the body, so it returns the original read/reset error, which is already classified as ambiguous (pinned, no failover). A regression test: send over a keep-alive connection and close the server side after it reads the request; expect `Ambiguous: true`.

## 2. Second operator replay exits 0 (small, matters for the Ops Console Replay button)

`delivery-worker replay EVENT OPERATOR` run a second (and third) time for the same event exits 0 with no error. Safety holds: Temporal shows a single `delivery-replay/EVENT` run and the poison was delivered once. But an operator (or the Ops Console) is told "started" on a duplicate click.

Likely cause: `replay.go` sets `WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE`, but the Go SDK's `ExecuteWorkflow` swallows the "already started" error and returns the existing run unless `StartWorkflowOptions.WorkflowExecutionErrorWhenAlreadyStarted: true` is set.

## 3. Ambiguous sends freeze the account for the whole outage (design trade-off, team call)

T1 (primary adapter stopped for 5 min): 8 events were in flight when it stopped. Their sends were `ambiguous`, so per the README they're pinned to sms_primary and never fail over. Those 8 accounts (16% of the 50) were blocked until primary came back: p99 278 s, and 1–3 OTPs expired per run. Everything else failed over within seconds. This is documented and avoids duplicates, but it's worth deciding on purpose. Options: bound the pin (e.g. fail over after N seconds or half the TTL, accepting a small duplicate risk), or ask the adapter for the status of `event_id` before failing over.

## 4. 45 s stall after a worker crash (tuning)

T2: worker SIGKILLed at 02:46:09 and running again by 02:46:27, but deliveries resumed at **02:46:54, 45 s after the kill**. That's franz-go's default `SessionTimeout` (45 s): the new consumer can't get the partitions until Kafka expires the dead member. p50 was 35 s for that run. Options: `kgo.SessionTimeout(10*time.Second)`, or static membership `kgo.InstanceID(...)` so a restarted pod takes its partitions back right away.

## Harness changes on my side (Lane 3, not yours)

- New load generator `--target iggy` (now the default). The worker only delivers events with an `ingested_events` row, so the old `--target kafka` traffic never reached the adapters.
- T5 now uses the operator replay command (re-publishing is deduped at ingest).
- Fault tests are no longer graded on the normal-load p99 2 s SLO. T1 grades drain ≤ 120 s, and T6 grades p99 ≤ 30 s plus failover.
