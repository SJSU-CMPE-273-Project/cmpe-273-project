# Reliability Harness

**Owner:** Pooja (Lane 3) · Built for the **Nov 30 failure-testing review**.

| Part | Folder | What it does |
|---|---|---|
| Load generator | `load-generator/` | Replays realistic banking traffic (deposits, fraud alerts, OTPs) at normal and burst (10×) rates. |
| Chaos injector | `chaos/` | Scripts against Toxiproxy's API (`localhost:8474`), adapter fault modes and `docker compose kill/stop` to break things on cue. **T8 built:** `chaos/t8_slow_provider.sh`. |
| Oracle (fake phone) | `oracle/` | **Built.** Acts as the SMS/push/email "provider", records every message and checks the invariants. See `oracle/README.md`. |

## Invariants the oracle checks
- **No loss:** every `event_id` produced is delivered, expired, or in the DLQ.
- **No duplicates:** each `event_id` delivered at most once.
- **Order:** per account, delivered `sequence` never decreases.
- **Latency:** p50 / p99 from `occurred_at` to provider ack under target.

Each run prints a pass/fail table plus the numbers. Save the output in `docs/results/` for the README.

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
| Test | Fault | Chaos command (sketch) | Pass |
|---|---|---|---|
| T1 | SMS adapter down 5 min | `docker compose stop sms-adapter-primary` | 0 lost after recovery |
| T2 | Kill worker mid-send | `docker compose kill -s SIGKILL delivery-worker` | 0 dup, 0 lost |
| T3 | Replay last 1,000 events | load-generator `--replay 1000` | 0 duplicate sends |
| T4 | Fail msg N for one account | oracle rejects `event_id` N, then accepts | N, N+1, N+2 in order |
| T5 | Poison message | oracle always rejects one `event_id` | lands in DLQ; replay delivers once |
| T6 | Primary provider outage | Toxiproxy: disable `sms_primary` | failover; report failover time |
| T7 | 10× burst for 2 min | load-generator `--rate 10x --duration 120s` | 0 lost; report peak lag (back pressure: backlog grows, then drains) |
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
