# Ingest throughput and T7 load

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

## Nov 13, 2026 local results

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
