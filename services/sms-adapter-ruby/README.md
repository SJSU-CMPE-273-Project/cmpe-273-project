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

## What gets sent to the provider
`POST $PROVIDER_URL`, JSON with `event_id, account_id, sequence, occurred_at, recipient, body, provider, adapter_received_at`. Headers: `Idempotency-Key: <event_id>` and `traceparent`. Provider 2xx → accepted; 4xx → rejected; 5xx or connection error → unavailable; timeout → timeout.

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
