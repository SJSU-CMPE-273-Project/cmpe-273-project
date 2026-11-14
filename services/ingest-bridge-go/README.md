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
   disables admission. Persist rejected source positions, reason codes, and
   parseable event UUIDs in `ingest_rejections`. Logs contain counts, partition
   numbers, and reason codes.
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

A fresh database applies migrations 003 and 004 automatically. For an existing
Compose database, apply both before starting the updated bridge.

```sh
docker compose stop ingest-bridge
docker compose up -d postgres
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/init/003_ingest_recovery.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
  < infra/postgres/init/004_ingest_rejection_identity.sql
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

Rejections are keyed by Iggy's topic creation timestamp, partition, and offset.
A recreated topic can reuse offsets without inheriting earlier rejections.
Stop the bridge during Iggy volume or topic maintenance and restart it after
initialization so it reads the new topic metadata and starts from its group's
offsets. Migration 004 preserves old rejection rows for audit with an unknown
topic instance (timestamp zero). A replay of an uncommitted legacy rejection
is evaluated again because the original topic instance is unknown.

Look up a missing event from an Oracle report in Postgres.

```sql
SELECT rejected_at, reason, topic_created_at, partition_id, source_offset
FROM ingest_rejections
WHERE event_id = '<event_uuid>'
ORDER BY rejected_at DESC;
```

The Ops Console can read the cumulative rejection count with
`SELECT total FROM ingest_rejection_counts`. This includes old topic instances
and records whose UUID could not be recovered. Replaying a rejection within the
same topic instance counts once.

## Verify

Run `go test ./...` for contract validation, OTP expiry, and recovery with injected broker failures. Set
`BRIDGE_TEST_DATABASE_URL` to a PostgreSQL database where the test role can
create schemas, then run
`go test -count=1 -run TestCrashRecovery ./services/ingest-bridge-go/cmd/ingest-bridge`.
The test uses a temporary schema and injects failures before Kafka acceptance,
after Kafka acknowledgement, and during the Iggy commit. It also checks
completed replays, durable rejection reasons and IDs, the rejection count, and
offset reuse across topic instances.
