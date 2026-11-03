# SMS Adapter (Ruby)

**Owner:** _TBD_ · **Layer:** ⑤ Channel adapters

Wraps an SMS "provider". Runs as **two instances** (`sms-adapter-primary`, `sms-adapter-secondary`) from the same code, so the worker can fail over.

## Responsibilities
- `POST /send` with `{event_id, recipient, body}` → forwards to the provider; returns `200` on accept.
- For the demo the "provider" is the harness **oracle** (fake phone), so every delivered message is recorded and checked.
- Pass `event_id` through to the provider as its idempotency key.
- Mask `recipient` and `body` in all logs (PII guardrail).
- `GET /health` for liveness.

## Connects to
| Dependency | Address |
|---|---|
| Oracle / fake phone | `oracle:8080` |

Port inside the container: `8080` (Toxiproxy forwards to it).

## Owned tests
- T1 Consumer down (stop this adapter for 5 min, all messages delivered after recovery).
- T6 Provider outage (primary down → secondary takes over).
