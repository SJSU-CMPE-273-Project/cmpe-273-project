# Fraud Engine producer (Python)

**Owner:** _TBD_ · **Layer:** ① Producers

Scores card transactions and emits `FRAUD_ALERT` notifications with `PRIORITY_HIGH`.

## Responsibilities
- Read a synthetic transaction feed; flag suspicious ones (simple rules are fine).
- Assign `event_id` and per-account `sequence`; attach a W3C `traceparent`.
- Publish `NotificationRequested` to Iggy stream `relayguard`, topic `requests`.
- Provide a **burst mode** ("fraud wave") used by test T7.

## Contract
Generate Python classes: `python -m grpc_tools.protoc -I contracts/proto --python_out=. contracts/proto/relayguard/v1/notification.proto`

## Connects to
| Dependency | Address (inside compose) | Address (laptop) |
|---|---|---|
| Iggy (TCP) | `iggy:8090` | `localhost:8090` |
| OTLP traces (optional) | `jaeger:4317` | `localhost:4317` |

## Owned tests
- T7 Burst (10× event rate for 2 minutes).
