# Failure-test run 2026-11-16-1848

Commit `d52e974`. Target and rate per test are in each file's header.

| Test | Result | Took | Verdict line | Output |
|---|---|---|---|---|
| T0 | PASS | 73s | PASS | [T0.txt](T0.txt) |
| T1 | SKIP | 2s | SKIP (needs delivery-worker (Lane 2) running in compose; nothing to retry, fail over or dead-letter without it) | [T1.txt](T1.txt) |
| T2 | SKIP | 1s | SKIP (needs delivery-worker (Lane 2) running in compose; nothing to retry, fail over or dead-letter without it) | [T2.txt](T2.txt) |
| T3 | PASS | 32s | PASS | [T3.txt](T3.txt) |
| T4 | SKIP | 1s | SKIP (needs delivery-worker (Lane 2) running in compose; nothing to retry, fail over or dead-letter without it) | [T4.txt](T4.txt) |
| T5 | SKIP | 0s | SKIP (needs delivery-worker (Lane 2) running in compose; nothing to retry, fail over or dead-letter without it) | [T5.txt](T5.txt) |
| T6 | SKIP | 1s | SKIP (needs delivery-worker (Lane 2) running in compose; nothing to retry, fail over or dead-letter without it) | [T6.txt](T6.txt) |
| T7 | PASS | 185s | PASS | [T7.txt](T7.txt) |
| T8 | PASS | 6s | PASS | [T8.txt](T8.txt) |
