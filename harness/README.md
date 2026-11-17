# Reliability Harness

**Owner:** Pooja (Lane 3) · Built for the **Nov 30 failure-testing review**.

| Part | Folder | What it does |
|---|---|---|
| Load generator | `load-generator/` | **Built.** Sends realistic banking traffic (deposits, fraud alerts, OTPs) to Kafka (the real path) or straight to an adapter, in phases such as `30s@20 2m@200`. Replays the same `event_id`s for T3. See `load-generator/README.md`. |
| Chaos injector | `chaos/` | **Built.** One script per test (`t0_baseline.sh` … `t8_slow_provider.sh`) plus `run_all.sh`. They break things through Toxiproxy's API (`localhost:8474`), adapter fault modes, oracle rules and `docker compose kill/stop`. |
| Oracle (fake phone) | `oracle/` | **Built.** Acts as the SMS/push/email "provider", records every message and checks the invariants. See `oracle/README.md`. |

## Invariants the oracle checks
- **No loss:** every `event_id` produced is delivered, expired, or in the DLQ.
- **No duplicates:** each `event_id` delivered at most once.
- **Order:** per account, delivered `sequence` never decreases.
- **Latency:** p50 / p99 from `occurred_at` to provider ack under target.

Each run prints a pass/fail table plus the numbers.

## Running the tests
```bash
make up                                  # platform + adapters + oracle
harness/chaos/run_all.sh                 # T0-T8 -> docs/results/<timestamp>/ (+ summary.md)
harness/chaos/run_all.sh t3 t7           # only some
TARGET=adapter harness/chaos/run_all.sh  # adapter-level variants (T0, T3, T7) work without the worker
```
Each script exits **0 PASS, 1 FAIL, 2 SKIP**. A test reports SKIP, not FAIL, when something it needs isn't running. Today that's mostly the delivery worker (Lane 2). Once `delivery-worker` runs in compose, the same scripts run end to end with no changes. Set `WORKER_SERVICE` if its compose name differs, and `WORKER_GROUP` for its Kafka consumer group (T7 lag).

Knobs (env): `RATE`, `OUTAGE_SECONDS` (T1 300, T6 45), `DOWN_SECONDS` (T2), `COUNT` (T3), `FAIL_TIMES` (T4), `BURST_SECONDS` (T7), `DRAIN_TIMEOUT`, `ORACLE_ADMIN_TOKEN`.

## Service level objectives (draft, for team review)
The oracle measures the SLIs (service level indicators); these are the targets it grades against. **The numbers are proposals, not measured yet.** Agree on them as a team before the Nov 30 review.

| SLI (what we measure) | SLO (target) | Kind | Where it comes from |
|---|---|---|---|
| Events accounted for (delivered, expired or DLQ) | **100%** | invariant: any miss fails | oracle `no loss` |
| Events delivered more than once | **0** | invariant | oracle `no duplicates` |
| Per-account order violations | **0** | invariant | oracle `per-account order` |
| OTPs delivered after `expires_at` | **0** | invariant | oracle `no late delivery` |
| End-to-end latency, normal load | **p99 ≤ 2 s** | SLO | oracle `latency` (`P99_TARGET_MS`) |
| End-to-end latency during a provider outage (T6) | p99 ≤ 30 s (proposed) | SLO | oracle latency during T6 |
| Delivered without going to the DLQ, normal load | ≥ 99.9% (proposed) | SLO | DLQ count / total |
| Backlog drained after a 5-min outage (T1) | ≤ 2 min (proposed) | SLO | time to last delivery |

An **SLA** is the external promise built on these, for example "fraud alerts delivered within 30 s, 99.9% of the month". We'd state it on the landing page, but a class project has no customer to owe penalties to.

## Test matrix (from the proposal)
| Test | Fault | Script | Pass |
|---|---|---|---|
| T0 | none (baseline, 60 s at 20/s) | `chaos/t0_baseline.sh` | all invariants, p99 under target |
| T1 | SMS adapter down 5 min | `chaos/t1_adapter_down.sh` (`docker compose stop`) | 0 lost after recovery |
| T2 | Kill worker mid-send | `chaos/t2_kill_worker.sh` (`kill -s SIGKILL`) | 0 dup, 0 lost |
| T3 | Replay last 1,000 events | `chaos/t3_replay.sh` (`loadgen replay --last 1000`) | 0 duplicate sends |
| T4 | Fail msg N for one account | `chaos/t4_fail_one.sh` (oracle fails N twice, then accepts) | N, N+1, N+2 in order |
| T5 | Poison message | `chaos/t5_poison.sh` (oracle always rejects one `event_id`) | lands in DLQ; replay delivers once |
| T6 | Primary provider outage | `chaos/t6_provider_outage.sh` (Toxiproxy disables `sms_primary`) | failover; report failover time |
| T7 | 10× burst for 2 min | `chaos/t7_burst.sh` (`--phase 30s@20 --phase 2m@200 --phase 30s@20`) | 0 lost; report peak lag (back pressure: backlog grows, then drains) |
| T8 | Slow provider (bulkhead) | `chaos/t8_slow_provider.sh`: primary provider hangs, 20 parallel sends | overflow gets 429 at once; primary `/health` and secondary stay fast |

Toxiproxy examples:
```bash
# take the primary SMS provider "down"
curl -s -X POST localhost:8474/proxies/sms_primary -d '{"enabled": false}'
# add 3s latency instead
curl -s -X POST localhost:8474/proxies/sms_primary/toxics \
  -d '{"name":"slow","type":"latency","attributes":{"latency":3000}}'
# restore
curl -s -X POST localhost:8474/proxies/sms_primary -d '{"enabled": true}'
curl -s -X DELETE localhost:8474/proxies/sms_primary/toxics/slow
```
