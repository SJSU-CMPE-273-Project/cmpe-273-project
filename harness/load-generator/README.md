# Load generator

**Owner:** Pooja (Lane 3) · Python 3.12

Sends realistic banking notifications (deposits, OTPs, fraud alerts, low-balance warnings) at a set rate. It tells the oracle about every event before sending it, so the oracle can tell "lost" apart from "never sent". The chaos scripts in `../chaos/` drive it. You rarely run it by hand.

## Targets

| `--target` | Sends to | Needs | Use it for |
|---|---|---|---|
| `kafka` (default) | `notifications.requested`, raw Protobuf, key = `account_id` (the format in `contracts/README.md`) | the delivery worker | the real T1–T7 |
| `adapter` | an adapter's `POST /send`, through Toxiproxy by default (`toxiproxy:21001`) | adapters + oracle | load-testing the adapters today, before the worker exists |
| `none` | nowhere, counts only | nothing | dry runs |

**Why Kafka and not Iggy:** writing straight to Kafka tests the delivery path on its own. If a test fails, the bug is in the worker or the adapters, not in ingest. Lane 1 tests the bridge separately.

**What adapter mode is not:** it's a stand-in client, not the worker. It keeps per-account order (one sender per account) and retries with the same `event_id`, but it has no failover, no DLQ and no TTL check. On a 429 it waits `Retry-After` and tries again without using up a retry attempt, because 429 means "slow down", not "broken". The worker should behave the same way.

## Events
- Every run uses its own accounts, `lg-<run-tag>-0001` and up, so sequences start at 1 and never collide with the real producers or an earlier run.
- `event_id` is a UUIDv4, `sequence` goes up by one per account, `occurredAt` is the send time. OTPs expire after 5 minutes and fraud alerts after 30 minutes.
- Phone numbers are `+1408555 01xx`, which is reserved for fiction. Only ids and times go to the oracle, never the number or the text.
- The Kafka bytes come from Python classes **generated from `contracts/proto` at build time**, so a contract change can't silently drift.

## Commands
Run inside the compose network:
```bash
alias lg='docker compose --profile harness run --rm -T load-generator'

lg run --target kafka --rate 20 --duration 60                      # 60 s at 20/s
lg run --target kafka --phase 30s@20 --phase 2m@200 --phase 30s@20  # 10x burst (T7)
lg run --target adapter --phase 30s@50 --reset-oracle               # adapters only, fresh oracle run
lg run ... --out /runs/x.jsonl                                      # keep the events for a replay
lg replay --from /runs/x.jsonl --last 1000 --target kafka           # same event_ids again (T3)
lg plan --count 3 --accounts 1 --out /runs/t4.jsonl                 # pick event_ids before sending (T4/T5)
lg replay --from /runs/t4.jsonl --fresh-timestamps                  # then send the plan
```
Other options: `--accounts 50`, `--mix deposit=0.6,otp=0.3,fraud=0.1`, `--seed` (the same seed and run tag give the same events), `--oracle ''` to skip registering. `/runs` is a named volume, so files survive between runs.

The last line on stdout is a JSON summary (`events`, `achieved_rate`, `max_lateness_s`, sink counts such as `accepted`, `http_429`, `peak_backlog`, `acked`). Logs go to stderr. The exit code is 1 if any event couldn't be sent.

If `max_lateness_s` gets large, the generator itself fell behind schedule. Treat that run's numbers with care.

## Tests
```bash
docker build --build-context contracts=contracts --target test -t loadgen:test harness/load-generator
docker run --rm loadgen:test    # 21 tests
```
The tests cover: contract shape, per-account sequences, the Protobuf round trip (no Confluent framing, unknown fields rejected), registering with the oracle before sending, 429 handling, per-account order under concurrency, and retries after a refused connection.
