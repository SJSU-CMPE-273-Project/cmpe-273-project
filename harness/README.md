# Reliability Harness

**Owner:** _TBD_ · Built for the **Nov 30 failure-testing review**.

| Part | Folder | What it does |
|---|---|---|
| Load generator | `load-generator/` | Replays realistic banking traffic (deposits, fraud alerts, OTPs) at normal and burst (10×) rates. |
| Chaos injector | `chaos/` | Scripts against Toxiproxy's API (`localhost:8474`) and `docker compose kill/stop` to break things on cue. |
| Oracle (fake phone) | `oracle/` | Acts as the SMS/push "provider". Records every message it receives and checks the invariants. |

## Invariants the oracle checks
- **No loss:** every `event_id` produced is delivered, expired, or in the DLQ.
- **No duplicates:** each `event_id` delivered at most once.
- **Order:** per account, delivered `sequence` never decreases.
- **Latency:** p50 / p99 from `occurred_at` to provider ack under target.

Each run prints a pass/fail table plus the numbers. Save the output in `docs/results/` for the README.

## Test matrix (from the proposal)
| Test | Fault | Chaos command (sketch) | Pass |
|---|---|---|---|
| T1 | SMS adapter down 5 min | `docker compose stop sms-adapter-primary` | 0 lost after recovery |
| T2 | Kill worker mid-send | `docker compose kill -s SIGKILL delivery-worker` | 0 dup, 0 lost |
| T3 | Replay last 1,000 events | load-generator `--replay 1000` | 0 duplicate sends |
| T4 | Fail msg N for one account | oracle rejects `event_id` N, then accepts | N, N+1, N+2 in order |
| T5 | Poison message | oracle always rejects one `event_id` | lands in DLQ; replay delivers once |
| T6 | Primary provider outage | Toxiproxy: disable `sms_primary` | failover; report failover time |
| T7 | 10× burst for 2 min | load-generator `--rate 10x --duration 120s` | 0 lost; report peak lag |

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
