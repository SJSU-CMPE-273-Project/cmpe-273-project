# Ops Console (TypeScript / React + small Node API)

**Owner:** _TBD_ · **Layer:** Shared platform

What we show the professor live during the failure demo.

## Screens
1. **Delivery timeline** per account: sequence, state, channel, attempts.
2. **DLQ view**: dead-lettered messages with full attempt history.
3. **Replay** button: re-publishes the original event **with the same `event_id`** (idempotent, can't double-send).
4. **Guardrails**: per-producer kill switch and rate limits (`producer_controls`, `guardrail_settings`).
5. **Live metrics**: delivered / retrying / DLQ counts, ingest rejections, p99 latency.

Read the ingest rejection count from Postgres with
`SELECT total FROM ingest_rejection_counts`. The view returns one cumulative
count across all retained topic instances, including rows without a parseable
event ID.

## Connects to
| Dependency | Address |
|---|---|
| Postgres (read model) | `postgres:5432` |
| Kafka (replay publish) | `kafka:9092` |
| Temporal UI (link out) | `http://localhost:8233` |
