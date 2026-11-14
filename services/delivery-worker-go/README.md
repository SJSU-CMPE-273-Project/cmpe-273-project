# Delivery Worker (Go, Temporal)

**Owner:** _TBD_ · **Layer:** ④ Durable delivery

Lane 2 owns in-order delivery and at-most-once sends per event ID.

## Design
- A Kafka consumer on `notifications.requested` calls **`SignalWithStartWorkflow`** with `workflow_id = account_id`. One long-running **entity workflow per account**.
- The account workflow keeps a queue ordered by `sequence` and delivers **one message at a time**, so N+1 waits for N. Other accounts are separate workflows and are never blocked.
- Each send is a Temporal **activity**. The delivery ledger and provider call use the original `event_id` as the idempotency key.
- Retry policy: exponential backoff with jitter → after `max_attempts_per_channel` fail over (SMS primary → SMS secondary → push/email) → then publish a `DeadLetter` to `notifications.dlq`.
- Check `expires_at` before every attempt (TTL guardrail). Expired → `EXPIRED`, not sent.
- Use `ContinueAsNew` after ~1,000 messages so workflow history stays small.

## Bridge handoff

Aaron's acceptance is pending. The bridge can publish the same `event_id`
twice if Kafka acknowledges before a bridge crash and Postgres still has the
event marked pending. Kafka idempotent writes cover producer retries within
the same session.

Lane 2 must collapse duplicate Kafka records and duplicate Temporal signals
into one delivery, including while a send is in flight and after
`ContinueAsNew`. Use an atomic claim on the `deliveries.event_id` ledger and
pass the same key through every attempt. If a provider may have accepted a
send before a worker crash or timeout, reconcile that attempt or retry through
an endpoint that deduplicates the same event ID. Failover after an ambiguous
result also requires shared provider deduplication or confirmation that the
first provider did not send.

Acceptance test: submit the same Kafka event twice, including while delivery
is in flight, and assert one provider send. Repeat with a duplicate after
delivery and with a worker restart after provider acceptance.

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
