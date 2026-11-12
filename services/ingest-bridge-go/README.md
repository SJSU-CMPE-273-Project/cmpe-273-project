# Ingest Bridge (Go)

The bridge consumes the six partitions of Iggy `relayguard/requests` as the
`ingest-bridge` group with auto-commit disabled. A Postgres advisory lock keeps
one active bridge session per database. Each partition advances in broker order,
including when Iggy assigns offsets with gaps.

1. Validate the JSON contract, account partition, and expiry fields. OTP events
   require an expiry after occurrence and within five minutes. The delivery
   worker owns the wall-clock expiry check, so an OTP that ages during an outage
   can still reach the delivery ledger and be recorded as expired.
2. Check `producer_controls.enabled` for new events. A missing control row
   disables admission. Persist rejected source positions and reason codes in
   `ingest_rejections`. Logs contain counts, partition numbers, and reason codes.
3. Insert a `pending` row with a SHA-256 fingerprint of the deterministic
   Protobuf message. A matching pending row resumes from the retained Iggy
   payload, even if the producer has since been disabled. Reusing an event ID
   with changed content or using an occupied account sequence with a different
   event ID is rejected. An identical `published` event adds zero Kafka records.
4. Publish the raw Protobuf to `notifications.requested`, keyed by `account_id`,
   using all in-sync replica acknowledgements and franz-go's idempotent producer.
   After Kafka acknowledges, mark the row `published`, then store the last
   successfully processed Iggy offset. Infrastructure errors stop polling and
   trigger a retry from persisted offsets after a one-second delay.

Kafka acknowledgement and the Postgres state change are an **at-least-once**
boundary. A crash between them can produce two Kafka records for one
`event_id`. Lane 2 must make those records lead to at most one send. See the
[delivery worker handoff](../delivery-worker-go/README.md) and the
[Kafka producer guarantees](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).

## Run locally

A fresh database applies migration 003 automatically. For an existing Compose
database, apply it before starting the bridge.

```sh
docker compose up -d postgres
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/init/003_ingest_recovery.sql
docker compose up -d --build ingest-bridge
```

Migration 003 can be rerun. If an existing database has rows from the old
schema, reconcile them with Kafka and the source before migrating. The old
rows have no content fingerprint or publication evidence and the migration
aborts rather than guessing their state.

To run on the host with local services, use
`go run ./services/ingest-bridge-go/cmd/ingest-bridge`. The defaults match
Compose's published ports. Set `DATABASE_URL`, `IGGY_TCP_ADDRESS`,
`IGGY_ROOT_USERNAME`, `IGGY_ROOT_PASSWORD`, and comma-separated `KAFKA_BROKERS`
as needed.

The local bridge checks that the Iggy topic uses persisted messages and offsets
with unlimited age and size retention. Keep the `iggydata` volume and provision
disk for the expected backlog. Keep the retained messages, offsets, and bridge
ledger together during recovery.

## Verify

Run `go test ./...` for contract validation, OTP expiry, and recovery with injected broker failures. Set
`BRIDGE_TEST_DATABASE_URL` to a PostgreSQL database where the test role can
create schemas, then run
`go test -count=1 -run TestCrashRecovery ./services/ingest-bridge-go/cmd/ingest-bridge`.
The test uses a temporary schema and injects failures before Kafka acceptance,
after Kafka acknowledgement, and during the Iggy commit. It also checks
completed replays and durable rejection reasons.
