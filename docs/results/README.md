# Failure-test results

One folder per run of `harness/chaos/run_all.sh`. Each has a `summary.md` and one file per test, holding the full output and the oracle's report.

## 2026-11-16-1848: adapter level, before the delivery worker exists
Run with `TARGET=adapter`. Traffic went from the load generator to Toxiproxy, then the SMS adapter, then the oracle. **No worker was in the loop**, so this run shows the adapters, the back pressure and provider idempotency working. It does not show retries, failover or the DLQ.

| Test | Result | What it shows |
|---|---|---|
| T0 baseline | PASS | 1,200 events at 20/s: p50 7 ms, p99 67 ms. This is the floor that end-to-end latency will be added on top of. |
| T3 replay | PASS | 1,000 events, then the same 1,000 again: 1,000 delivered, 1,000 absorbed as idempotent replays, 0 duplicates. This proves provider-level idempotency only. The worker's own dedup needs the Kafka run. |
| T7 10× burst | PASS | 25,200 events (20/s, then 200/s for 2 min): 0 lost, 0 duplicates, order kept. The adapter pushed back with 131 × 429; the backlog peaked at 2,049 and then drained. p99 rose to 12.5 s during the burst. That's reported, not graded: the 2 s SLO is for normal load. |
| T8 slow provider | PASS | The bulkhead holds. Overflow got 429 within 0.14 s, `/health` stayed up, and the secondary adapter was unaffected. |
| T1, T2, T4, T5, T6 | SKIP | They need `delivery-worker` (Lane 2). The scripts are ready; rerun with the default `TARGET=kafka` once it's in compose. |

Postgres wasn't running for this run (a local port clash), so the oracle's ledger line says "unavailable". None of these tests produce expired or dead-lettered events, so it doesn't change any result.

## Runs on Nov 22 and Nov 25: end to end, with the delivery worker
These ran the full path: load generator → Iggy → bridge → Kafka → delivery worker (Temporal) → Toxiproxy → adapters → oracle. The worker isn't on `main` yet, so each ran on a **local merge of `main` with the Lane 2 branch `lane2/dlq`** (PRs #34–#37). The commit in each `summary.md` is that local merge and is not on GitHub:

| Folder | Code under test |
|---|---|
| `2026-11-22-0915` | `main` @ `be5eff3` + `lane2/dlq` @ `079c8bc` |
| `2026-11-25-1657`, `2026-11-25-1701` | `main` @ `be5eff3` + `lane2/dlq` @ `835823e` (eenya's T5/T6 fix) |

Latency is graded only in T0 and T3. The other tests break things on purpose, so they report it but don't grade it.

### 2026-11-22-0915: T1 and T5
| Test | Result | What it shows |
|---|---|---|
| T1 adapter down 5 min | PASS | 1,799 delivered + 1 expired (an OTP past its TTL), 0 duplicates, order kept. Drained 42.6 s after the adapter came back (SLO ≤ 120 s). |
| T5 poison message | FAIL | The poison went to the DLQ and the first replay delivered it once, but **a second replay was accepted**. Reported on PR #37; fixed in `835823e`. |

### 2026-11-25-1657: T5 and T6 after the fix
| Test | Result | What it shows |
|---|---|---|
| T5 poison message | PASS | First replay delivered once; **second replay refused**. |
| T6 primary provider down 45 s | PASS | 900/900 delivered, 0 duplicates, order kept. Failover in 0.56 s; p99 6.8 s during the outage (SLO ≤ 30 s). |

### 2026-11-25-1701: full T0–T7
| Test | Result | What it shows |
|---|---|---|
| T0 baseline | FAIL (latency) | 1,200/1,200, 0 duplicates, in order. p99 10.6 s against the 2 s target. |
| T1 adapter down 5 min | PASS | 1,799 delivered + 1 expired, 0 duplicates. Drained 40.7 s after restart. |
| T2 kill worker | PASS | 1,200/1,200 after SIGKILL, 0 duplicates. |
| T3 replay 1,000 | FAIL (latency) | 1,000 replayed → exactly 1,000 sends, 0 duplicates. p99 36.7 s. |
| T4 fail one message | PASS | N, N+1, N+2 delivered in order. |
| T5 poison message | PASS | DLQ, one replay, second replay refused. |
| T6 provider outage | PASS | 900/900, 0 duplicates, failover in 0.39 s. |
| T7 10× burst | FAIL (loss check) | 4,893 of 25,200 delivered before the drain timeout, 0 duplicates, in order. The rest were still queued in Iggy/Kafka, not lost. Peak worker lag 5,204. |

**Duplicates and ordering errors: 0 in every test.** The latency and T7 failures share one cause: the worker reads Kafka one record at a time, with a Temporal call and a synchronous commit per record (about 10 messages/s). Details and a suggested fix are on PR #37; T7 gets rerun once that lands.
