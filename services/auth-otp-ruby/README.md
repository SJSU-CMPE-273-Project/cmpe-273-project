# Auth / OTP producer (Ruby)

**Owner:** _TBD_ · **Layer:** ① Producers

Issues one-time passcodes. OTPs are useless after 5 minutes, so every event carries `expires_at`.

## Responsibilities
- Generate `OTP` events with `PRIORITY_HIGH` and `expires_at = occurred_at + 5 min`.
- Assign `event_id` and per-account `sequence`.
- Publish `NotificationRequested` to Iggy (HTTP API is easiest from Ruby: `POST` to `iggy:3000`).

## Contract
Generate Ruby classes with `grpc-tools`: `grpc_tools_ruby_protoc -I contracts/proto --ruby_out=. contracts/proto/relayguard/v1/notification.proto`

## Connects to
| Dependency | Address (inside compose) | Address (laptop) |
|---|---|---|
| Iggy (HTTP) | `http://iggy:3000` | `http://localhost:3000` |

## Owned tests
- Guardrail: OTP TTL. An OTP held back by an outage must be **expired, not sent late**.
