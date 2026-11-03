# Delivery Worker (Go, Temporal)

**Owner:** _TBD_ · **Layer:** ④ Durable delivery

The heart of RelayGuard. Turns Kafka events into exactly-once, in-order sends.

## Design
- A Kafka consumer on `notifications.requested` calls **`SignalWithStartWorkflow`** with `workflow_id = account_id`. One long-running **entity workflow per account**.
- The account workflow keeps a queue ordered by `sequence` and delivers **one message at a time**, so N+1 waits for N. Other accounts are separate workflows and are never blocked.
- Each send is a Temporal **activity** whose idempotency key is `event_id`. Before calling a provider it claims the row in `deliveries`; if already `delivered` → skip.
- Retry policy: exponential backoff with jitter → after `max_attempts_per_channel` fail over (SMS primary → SMS secondary → push/email) → then publish a `DeadLetter` to `notifications.dlq`.
- Check `expires_at` before every attempt (TTL guardrail). Expired → `EXPIRED`, not sent.
- Use `ContinueAsNew` after ~1,000 messages so workflow history stays small.

## Connects to
| Dependency | Address (inside compose) |
|---|---|
| Temporal | `temporal:7233`, namespace `relayguard`, task queue `delivery` |
| Kafka | `kafka:9092` |
| Postgres | `postgres:5432` |
| SMS primary (via Toxiproxy) | `toxiproxy:21001` |
| SMS secondary (via Toxiproxy) | `toxiproxy:21002` |
| Push/email (via Toxiproxy) | `toxiproxy:21003` |

Always call adapters **through Toxiproxy**, so the chaos harness can break them.

## Owned tests
- T2 `kill -9` mid-send (0 duplicates, 0 lost).
- T4 Ordering, T5 Poison → DLQ, T6 Provider outage / failover.
