# Load generator
**Owner:** Pooja (Lane 3) · Python 3.12

Sends realistic banking notifications (deposits, OTPs, fraud alerts, low-balance warnings) at a set rate. It tells the oracle about every event before sending it, so the oracle can tell "lost" apart from "never sent". The chaos scripts in `../chaos/` drive it. You rarely run it by hand.

This folder holds **two tools**:
- **`loadgen` (Python, Lane 3)**: delivery-path load and chaos traffic, documented first below. Used by `../chaos/`.
- **`main.go` (Go, Lane 1)**: ingest throughput through Iggy and the bridge, documented in [Ingest throughput (Go)](#ingest-throughput-go-lane-1) at the end.

## Targets

| `--target` | Sends to | Needs | Use it for |
|---|---|---|---|
| `iggy` (default) | Iggy `relayguard/requests`, ProtoJSON, on partition `sha256(account)[:4] % 6`, exactly like the real producers | Iggy, ingest bridge, Kafka, the delivery worker | the real T0–T7 (full producer → phone path) |
| `kafka` | `notifications.requested`, raw Protobuf, key = `account_id` (the format in `contracts/README.md`) | Kafka | Kafka-only checks. **The worker won't deliver these** (see below) |
| `adapter` | an adapter's `POST /send`, through Toxiproxy by default (`toxiproxy:21001`) | adapters + oracle | adapter-level tests (T0, T3, T7 with `TARGET=adapter`) |
| `none` | nowhere, counts only | nothing | dry runs |

**Why Iggy and not Kafka:** the worker only delivers events that already have a row in `ingested_events`, and only the ingest bridge writes that row ("ingest is the admission authority", see `services/delivery-worker-go/README.md`). An event written straight to Kafka fails the `deliveries → ingested_events` foreign key and is never sent. Going through Iggy also means the tests cover ingest, which the Nov 30 demo needs to show.

**The partition has to match.** The bridge recomputes `sha256(account_id)`, first 4 bytes big-endian, mod 6, and rejects a mismatch as `wrong_partition`. That keeps each account on one partition, so its messages stay in order. `wire.iggy_partition` must stay bit-for-bit the same as `tools/demo-producer.py` and the bridge.

**Replays through Iggy are deduped at ingest.** Re-sending an `event_id` the bridge already published is dropped as a duplicate (or rejected as `event_id_conflict` if the timestamps changed), so it never reaches the worker. T3 relies on that. A DLQ replay (T5) uses the worker's operator command instead: `docker compose exec delivery-worker /delivery-worker replay EVENT_ID OPERATOR`.

**What adapter mode is not:** it's a stand-in client, not the worker. It keeps per-account order (one sender per account) and retries with the same `event_id`, but it has no failover, no DLQ and no TTL check. On a 429 it waits `Retry-After` and tries again without using up a retry attempt, because 429 means "slow down", not "broken". The worker should behave the same way.

## Events
- Every run uses its own accounts, `lg-<run-tag>-0001` and up, so sequences start at 1 and never collide with the real producers or an earlier run.
- `event_id` is a UUIDv4, `sequence` goes up by one per account, `occurredAt` is the send time. OTPs expire after 5 minutes and fraud alerts after 30 minutes.
- Phone numbers are `+1408555 01xx`, which is reserved for fiction. Only ids and times go to the oracle, never the number or the text.
- The Iggy JSON and Kafka bytes come from Python classes **generated from `contracts/proto` at build time**, so a contract change can't silently drift.

## Commands
Run inside the compose network:
```bash
alias lg='docker compose --profile harness run --rm -T load-generator'

lg run --rate 20 --duration 60                                     # 60 s at 20/s, through Iggy
lg run --phase 30s@20 --phase 2m@200 --phase 30s@20                # 10x burst (T7)
lg run --target adapter --phase 30s@50 --reset-oracle               # adapters only, fresh oracle run
lg run ... --out /runs/x.jsonl                                      # keep the events for a replay
lg replay --from /runs/x.jsonl --last 1000                          # same event_ids again (T3)
lg plan --count 3 --accounts 1 --out /runs/t4.jsonl                 # pick event_ids before sending (T4/T5)
lg replay --from /runs/t4.jsonl --fresh-timestamps                  # then send the plan
```
Other options: `--accounts 50`, `--mix deposit=0.6,otp=0.3,fraud=0.1`, `--seed` (the same seed and run tag give the same events), `--oracle ''` to skip registering. `/runs` is a named volume, so files survive between runs.

The last line on stdout is a JSON summary (`events`, `achieved_rate`, `max_lateness_s`, sink counts such as `accepted`, `http_429`, `peak_backlog`, `acked`). Logs go to stderr. The exit code is 1 if any event couldn't be sent.

If `max_lateness_s` gets large, the generator itself fell behind schedule. Treat that run's numbers with care.

## Tests
```bash
docker build --build-context contracts=contracts --target test -t loadgen:test harness/load-generator
docker run --rm loadgen:test    # includes bounded public demo-trigger tests
```
The tests cover: contract shape, per-account sequences, the Protobuf round trip (no Confluent framing, unknown fields rejected), registering with the oracle before sending, 429 handling, per-account order under concurrency, and retries after a refused connection.

## Ingest throughput (Go, Lane 1)

**Owner:** Ynha (Lane 1) · Go

Run from the repository root against a test stack with Iggy, Kafka, Postgres
(migrations 003 and 004), and the ingest bridge. Each invocation uses a fresh
pool of 60 synthetic fraud accounts and one Iggy TCP connection. Each account
uses a monotonically increasing sequence and always routes to the same
partition. The generator sends partition-local batches of up to `--batch`
records (100 by default).

```sh
docker compose up -d --build ingest-bridge
go run ./harness/load-generator --rate normal --base-rate 10 --duration 30s
go run ./harness/load-generator --rate 10x --base-rate 10 --duration 120s
```

The default baseline is 10 messages/sec, so 10× targets 100 messages/sec.
Set `--base-rate` to the team's agreed baseline, or use `--rate 0` to send
unpaced for a short saturation test. The generator stops starting new batches
when the send window expires and allows in-flight batches to finish. Set
`DATABASE_URL`, `IGGY_TCP_ADDRESS`, `IGGY_ROOT_USERNAME`, and
`IGGY_ROOT_PASSWORD` for non-default connection settings.

The JSON result reports Iggy acknowledgements/sec, bridge publications/sec
during the send window, peak sampled backlog, and observed drain time. It
counts only this run's accounts. A bridge publication is a Postgres
`published` row, recorded after the Kafka acknowledgement. Backlog is sampled
about once a second during sending and every 250 ms while draining. Busy or
unavailable services can extend sample intervals. A drain timeout exits with
an error and prints the partial result.

For T7, start the delivery worker and Oracle, then add
`--oracle-url http://localhost:18080`. Set `ORACLE_ADMIN_TOKEN` if the Oracle
requires it. The generator registers each acknowledged batch at `/expected`.
Check the Oracle report after delivery drains for event loss, duplicates, and
ordering. The ingest measurements end at Kafka. T3 replay is tracked in
[#6](https://github.com/SJSU-CMPE-273-Project/cmpe-273-project/issues/6).

### Nov 13, 2026 local results

Iggy 0.9.0 and Kafka 3.9.1 ran in a local Podman VM. The generator, bridge, and
PostgreSQL 16 ran on the macOS host. The bridge processed one record at a
time. Oracle registration was enabled for a separate 19-event smoke run
during the two-minute measurement. The Oracle registered all 19 events.

| Load | Send window | Iggy ACK/sec | Bridge published/sec during send | Iggy ACKs / eventually published | Peak sampled backlog | Observed drain |
|---|---:|---:|---:|---:|---:|---:|
| Normal, target 10/sec | 5 s | 9.8 | 9.8 | 49 / 49 | 1 | Caught up at end of send |
| 10×, target 100/sec | 120 s | 100.0 | 69.6 | 11,999 / 11,999 | 3,651 | 52.3 s |

The producer sustained the burst target. The bridge fell behind during the
burst and published every acknowledged event after draining. Bridge-side
batching is a follow-up if the target is to keep up at 100 messages/sec. The
delivery worker is currently documented only, so delivery loss and ordering
checks for T7 remain pending.
