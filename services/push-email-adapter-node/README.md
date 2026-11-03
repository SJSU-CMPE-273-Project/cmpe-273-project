# Push / Email Adapter (Node.js)

**Owner:** _TBD_ · **Layer:** ⑤ Channel adapters

Last-resort fallback channel when both SMS providers are failing.

## Responsibilities
- `POST /send` with `{event_id, channel, recipient, body}` → mock push/email provider (the oracle).
- Same idempotency and PII-masking rules as the SMS adapter.
- `GET /health` for liveness.

Port inside the container: `8080`.

## Owned tests
- Fallback path: both SMS providers down → message arrives via push/email exactly once.
