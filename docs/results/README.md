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
