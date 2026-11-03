# Ingest Bridge (Go)

**Owner:** _TBD_ · **Layer:** ② Ingest

Moves events from Iggy into Kafka **without loss and without duplicates**.

## Responsibilities
1. Consume Iggy stream `relayguard`, topic `requests` (consumer group, manual offset commit).
2. Validate against `contracts/jsonschema/notification_requested.schema.json`. Invalid → reject + log (guardrail: schema validation).
3. Check `producer_controls.enabled` (guardrail: per-producer kill switch).
4. Dedup: `INSERT INTO ingested_events ... ON CONFLICT (event_id) DO NOTHING`. 0 rows → duplicate, skip.
5. Produce to Kafka `notifications.requested` with **key = `account_id`**, idempotent producer (`enable.idempotence=true`, `acks=all`).
6. Commit the Iggy offset **only after** Kafka acks.

> Crash between steps 5 and 6? Iggy redelivers, step 4 drops the duplicate. That is the whole trick.

## Connects to
| Dependency | Address (inside compose) |
|---|---|
| Iggy (TCP) | `iggy:8090` |
| Kafka | `kafka:9092` |
| Postgres | `postgres:5432` / db `relayguard` |

## Owned tests
- T3 Replay (0 duplicates reach Kafka).
- Bridge crash mid-batch (0 lost).
