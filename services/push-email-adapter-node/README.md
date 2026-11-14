# Push / Email Adapter (Node.js)

**Owner:** Pooja (Lane 3) · **Layer:** ⑤ Channel adapters

The **last channel** in the failover chain: SMS primary → SMS secondary → **push/email** → DLQ. One service handles both push and email. It is built on Node's own `http` module, and its only dependencies are OpenTelemetry packages. It is stateless: no disk writes, read-only container.

It uses **the same HTTP contract and fault modes as the [SMS adapter](../sms-adapter-ruby/README.md)**, so the delivery worker and the chaos scripts handle every channel the same way. The only additions are `channel` and an optional `subject`.

## HTTP contract (for the delivery worker)

Always call through Toxiproxy: `http://toxiproxy:21003`.

### `POST /send`
```json
{
  "event_id": "0b9a6c1e-…",          // required: idempotency key, forwarded to the provider
  "channel": "push",                  // "push" or "email" (CHANNEL_PUSH / CHANNEL_EMAIL also accepted).
                                      //   Omitted: an address with "@" goes to email, anything else to push.
  "recipient": "device-token-…",      // required: push token, or an email address for email
  "body": "Your code is 482913",      // required
  "subject": "Fraud alert",           // optional, email only
  "account_id": "acct-1001",          // optional: passed to the oracle for the order check
  "sequence": 42,                      // optional: passed to the oracle for the order check
  "occurred_at": "2026-11-13T18:05:00Z", // optional: passed to the oracle for latency
  "traceparent": "00-…"                // optional, or send it as the `traceparent` header
}
```

| Status | `status` | `retryable` | What the worker should do |
|---|---|---|---|
| 200 | `accepted` | false | Mark delivered. Response includes `provider_message_id`. |
| 400 | `invalid` | false | Bug in the request (bad JSON, missing field, `channel: "sms"`, email without `@`). Don't retry; dead-letter. |
| 422 | `rejected` | false | Provider said a permanent no. This is the last channel, so dead-letter. |
| 429 | `busy` | true | **Back pressure**: the adapter is at capacity. Wait `Retry-After` seconds, then retry with the same `event_id`. Not a provider failure, so it should not trip the circuit breaker. |
| 503 | `unavailable` | true | Temporary outage. Retry with backoff, then dead-letter. |
| 504 | `timeout` | true | Provider was too slow. Retry with the same `event_id`. |
| no response | — | — | Connection refused/reset (adapter down, or Toxiproxy cut it). Treat like 503. |

Every response body includes `event_id`, `provider` (`push` / `email`) and `retryable`.

### `GET /health`
Liveness. Also shows the fault mode, the bulkhead counters and whether the provider is simulated.

## Fault injection (for the chaos harness)

Same modes and API as the SMS adapter. One fault mode covers both channels.

| Mode | Response | Use for |
|---|---|---|
| `ok` | forward normally | happy path |
| `fail` | 503 | all channels down → DLQ (T6 worst case) |
| `timeout` | hang `timeout_seconds`, then 504 | slow provider |
| `reject` | 422 | permanent rejection |
| `flaky` | 503 with probability `fail_rate` | retries / backoff |

```bash
curl -X PUT localhost:18083/admin/fault -d '{"mode":"fail"}'
curl -X PUT localhost:18083/admin/fault -d '{"mode":"flaky","fail_rate":0.3}'
curl -X PUT localhost:18083/admin/fault -d '{"mode":"ok"}'      # restore
```
The starting mode comes from `FAULT_MODE`. A restart resets to it.
If `ADMIN_TOKEN` is set (set it when deployed), add `-H "Authorization: Bearer $TOKEN"`.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `PROVIDER_BASE_URL` | *(blank)* | Base URL of the provider. Push goes to `$PROVIDER_BASE_URL/push`, email to `/email`. Compose sets `http://oracle:8080`. Blank = **simulate**: accept and log. |
| `PROVIDER_OPEN_TIMEOUT` / `PROVIDER_READ_TIMEOUT` | `2` / `5` s | Timeouts for connecting to the provider and for its answer |
| `FAULT_MODE`, `FAIL_RATE`, `TIMEOUT_SECONDS` | `ok`, `0`, `30` | Starting fault settings |
| `ADMIN_TOKEN` | *(blank)* | Protects `/admin/*` when set |
| `MAX_IN_FLIGHT` | `32` | Bulkhead: max provider calls at once; more get 429. `0` = off. |
| `RETRY_AFTER_SECONDS` | `1` | `Retry-After` value sent with 429 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(blank)* | Turns on tracing, e.g. `http://jaeger:4318`, or the Instana agent later. Blank = off. |
| `OTEL_SERVICE_NAME` | `push-email-adapter` | Name shown in Jaeger |

## What gets sent to the provider
`POST $PROVIDER_BASE_URL/<channel>`, JSON with `event_id, account_id, sequence, occurred_at, recipient, body, subject (email), provider, adapter_received_at`. `provider` is `push` or `email`; the oracle records it as the channel the message arrived on. Headers: `Idempotency-Key: <event_id>` and `traceparent`. Provider 2xx → accepted; 4xx → rejected; 5xx or connection error → unavailable; timeout → timeout.

## Tracing (OpenTelemetry)
Off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. When it is on, the behaviour matches the SMS adapter, so a message that fails over from SMS to push is still **one trace** across the Go, Ruby, Node and Python services:
- Each `POST /send` is a server span, and the call to the provider is a child span. `traceparent` is passed on, so the oracle's span joins the same trace.
- If the worker puts `traceparent` only in the JSON body, the adapter still continues that trace.
- **Span attributes:** `relayguard.event_id`, `.provider`, `.result` and `.retryable`. Never the recipient or the text; `test/telemetry.test.js` checks this.
- **Logs:** each log line carries `trace_id`.
- `/health` is left out of tracing, so Docker's healthcheck doesn't bury real spans.

## Bulkhead and back pressure
Node runs on a single thread, so a slow provider can't use up threads the way it can in the Ruby adapter, and `/health` keeps answering either way. What a slow provider *does* pile up is open sockets, timers and request bodies held in memory. The adapter caps that at `MAX_IN_FLIGHT` provider calls (`src/bulkhead.js`). Overflow gets an **immediate 429 + `Retry-After`**, so the message waits in Kafka/Temporal instead of in this process. `GET /health` shows `bulkhead: {max_in_flight, in_flight, rejected}`.

## Guardrails
- **PII:** logs show `recipient` as `p***@example.com` (email) or `***9876` (push token), and `body` as `[redacted len=N]`. Error messages never echo request data.
- **Input limits:** the request body is capped at 64 KB, and `channel` must be `push` or `email`.
- **Least privilege:** non-root `node` user, read-only filesystem, production dependencies only, exact dependency versions locked in `package-lock.json`.

## Run and test
```bash
docker compose up -d --build push-email-adapter oracle toxiproxy
curl localhost:18083/health

# tests (Node 22+), locally or in Docker
cd services/push-email-adapter-node && npm ci && npm test
docker build --target test -t push-email-adapter:test services/push-email-adapter-node && docker run --rm push-email-adapter:test
```

## Owned tests
- **T6 fallback:** both SMS adapters `fail` (or cut in Toxiproxy) → the message arrives through push/email **exactly once**. If it also got through on SMS (for example, SMS timed out after the provider had already taken it), the oracle reports it as a duplicate, because both copies share one `event_id`.
- **All channels down:** all three adapters `fail` → the message goes to the DLQ, and nothing reaches the oracle.
