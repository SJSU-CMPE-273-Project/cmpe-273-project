# SMS Adapter (Ruby)

**Owner:** Pooja (Lane 3) · **Layer:** ⑤ Channel adapters

Wraps an SMS "provider". Runs as **two instances** (`sms-adapter-primary`, `sms-adapter-secondary`) from the same image, so the worker can fail over. Built with Sinatra + Puma; stateless (no disk writes, read-only container).

## HTTP contract (for the delivery worker)

Always call through Toxiproxy: primary `http://toxiproxy:21001`, secondary `http://toxiproxy:21002`.

### `POST /send`
```json
{
  "event_id": "0b9a6c1e-…",         // required: idempotency key, forwarded to the provider
  "recipient": "+14085550123",       // required
  "body": "Your code is 482913",     // required
  "account_id": "acct-1001",         // optional: passed to the oracle for the order check
  "sequence": 42,                     // optional: passed to the oracle for the order check
  "occurred_at": "2026-10-26T18:05:00Z", // optional: passed to the oracle for latency
  "traceparent": "00-…"               // optional, or send it as the `traceparent` header
}
```

| Status | `status` | `retryable` | What the worker should do |
|---|---|---|---|
| 200 | `accepted` | false | Mark delivered. Response includes `provider_message_id`. |
| 400 | `invalid` | false | Bug in the request. Don't retry; dead-letter. |
| 422 | `rejected` | false | Provider said a permanent no. Skip retries, go to the next channel / DLQ. |
| 429 | `busy` | true | **Back pressure**: the adapter is at capacity. Wait `Retry-After` seconds, then retry (same `event_id`) or fail over. Not a provider failure, so it should not trip the circuit breaker. |
| 503 | `unavailable` | true | Temporary outage. Retry with backoff, then fail over. |
| 504 | `timeout` | true | Provider was too slow. Retry (same `event_id`). |
| no response | — | — | Connection refused/reset (adapter down, or Toxiproxy cut it). Treat like 503. |

Every response body includes `event_id`, `provider` (`sms_primary` / `sms_secondary`) and `retryable`.

### `GET /health`
Liveness. Also shows the current fault mode and whether the provider is simulated.

## Fault injection (for the chaos harness)

Toxiproxy breaks the network. These modes break the provider's *answer*:

| Mode | Response | Use for |
|---|---|---|
| `ok` | forward normally | happy path |
| `fail` | 503 | provider outage (T6) |
| `timeout` | hang `timeout_seconds`, then 504 | slow provider |
| `reject` | 422 | permanent rejection |
| `flaky` | 503 with probability `fail_rate` | retries / backoff |

```bash
curl -X PUT localhost:18081/admin/fault -d '{"mode":"fail"}'                  # primary
curl -X PUT localhost:18082/admin/fault -d '{"mode":"flaky","fail_rate":0.3}'  # secondary
curl -X PUT localhost:18081/admin/fault -d '{"mode":"ok"}'                    # restore
```
The starting mode comes from `FAULT_MODE`. A restart resets to it.
If `ADMIN_TOKEN` is set (set it when deployed), add `-H "Authorization: Bearer $TOKEN"`.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `PROVIDER_NAME` | `sms_primary` | Name reported in responses and logs |
| `PROVIDER_URL` | *(blank)* | Where to forward. Blank = **simulate**: accept and log. This lets the happy path run before the oracle exists. Later: `http://oracle:8080/sms`. |
| `PROVIDER_OPEN_TIMEOUT` / `PROVIDER_READ_TIMEOUT` | `2` / `5` s | Timeouts when calling the provider |
| `FAULT_MODE`, `FAIL_RATE`, `TIMEOUT_SECONDS` | `ok`, `0`, `30` | Starting fault settings |
| `ADMIN_TOKEN` | *(blank)* | Protects `/admin/*` when set |
| `MAX_IN_FLIGHT` | `8` | Bulkhead: max provider calls at once; more get 429. Keep it below Puma's 16 threads. `0` = off. |
| `RETRY_AFTER_SECONDS` | `1` | `Retry-After` value sent with 429 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(blank)* | Turns on tracing, e.g. `http://jaeger:4318`. Blank = off. |
| `OTEL_SERVICE_NAME` | `sms-adapter` | Name shown in Jaeger (compose sets `sms-adapter-primary` / `-secondary`) |

## What gets sent to the provider
`POST $PROVIDER_URL`, JSON with `event_id, account_id, sequence, occurred_at, recipient, body, provider, adapter_received_at`. Headers: `Idempotency-Key: <event_id>` and `traceparent`. Provider 2xx → accepted; 4xx → rejected; 5xx or connection error → unavailable; timeout → timeout.

## Tracing (OpenTelemetry)
Off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. When on, each `POST /send` is a server span, and the call to the provider is a child span. The W3C `traceparent` is passed on, so the oracle's span joins the same trace.
- **Worker: send `traceparent` as an HTTP header.** If it is only in the JSON body, the adapter still continues that trace.
- **Span attributes:** `relayguard.event_id`, `.provider`, `.result` (accepted / rejected / unavailable / timeout) and `.retryable`. Never the phone number or the text; `test/telemetry_test.rb` checks this.
- **Logs:** each log line carries `trace_id`, so a log can be matched to its trace in Jaeger.

## Bulkhead and back pressure
A slow provider would otherwise tie up every Puma thread, until even `/health` stops answering. The adapter therefore runs at most `MAX_IN_FLIGHT` provider calls at once (`lib/bulkhead.rb`).
- **Overflow gets an immediate 429 + `Retry-After`** instead of queueing inside the adapter. The message waits safely in Kafka/Temporal, and the worker slows down. That is back pressure.
- **Slots are released even when a request halts or raises.**
- **`GET /health` shows `bulkhead: {max_in_flight, in_flight, rejected}`.**
- **Test:** `harness/chaos/t8_slow_provider.sh` (T8) floods a hanging provider and checks the overflow is refused at once, `/health` stays up and the secondary is unaffected.

## Guardrails
- **PII:** logs show `recipient` as `***0123` and `body` as `[redacted len=N]`. Bodies are not hashed, because short OTP codes could be brute-forced back from a hash.
- **Least privilege:** non-root user, read-only filesystem, no compilers in the runtime image.

## Run and test
```bash
docker compose up -d --build sms-adapter-primary sms-adapter-secondary toxiproxy
curl localhost:18081/health

# unit tests (Ruby 3.3 in Docker, no local Ruby needed)
docker build --target test -t sms-adapter:test services/sms-adapter-ruby && docker run --rm sms-adapter:test
```

## Owned tests
- T1 Consumer down: `docker compose stop sms-adapter-primary` for 5 min; every message is delivered after recovery.
- T6 Provider outage: primary down or `fail` → secondary takes over.
- T8 Slow provider is isolated: `harness/chaos/t8_slow_provider.sh` (bulkhead + back pressure).
