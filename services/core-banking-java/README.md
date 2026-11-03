# Core Banking producer (Java, Spring Boot)

**Owner:** _TBD_ · **Layer:** ① Producers

Simulates the bank's ledger. Emits deposit events that customers must see **in order** ("pending" before "confirmed").

## Responsibilities
- Generate deposit events per account: `DEPOSIT_PENDING`, then `DEPOSIT_CONFIRMED`.
- Assign `event_id` (UUID) and a **per-account, strictly increasing** `sequence`.
- Publish `NotificationRequested` to Iggy stream `relayguard`, topic `requests`.
- Retry publishing on error **with the same `event_id`** (the bridge dedups).

## Contract
`contracts/proto/relayguard/v1/notification.proto` → generate Java classes with the protobuf Maven/Gradle plugin.

## Connects to
| Dependency | Address (inside compose) | Address (laptop) |
|---|---|---|
| Iggy (TCP) | `iggy:8090` | `localhost:8090` |

## Owned tests
- T4 Ordering (pending → confirmed must arrive in order).
- T3 Replay (re-publish last N events).
