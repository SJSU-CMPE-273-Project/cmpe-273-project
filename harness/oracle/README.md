# Oracle (fake phone)

**Owner:** Pooja (Lane 3) · Python 3.12, FastAPI

The oracle stands in for every real provider (SMS primary, SMS secondary, push, email) and grades each test run against RelayGuard's promises. It is the judge for T1–T7.

## What it checks

| Check | Passes when | Notes |
|---|---|---|
| **no loss** | every expected event is delivered, expired or dead-lettered | Expired/dead-lettered states are read from Postgres `deliveries`. "Delivered" is never taken from the worker; only the oracle's own receipts count. |
| **no duplicates** | each `event_id` reaches the phone at most once | Across all providers and channels |
| **per-account order** | for each account, delivered `sequence` never goes backwards | Gaps are fine. Dead-lettered events are exempt, because a replay arrives late by design (T5). |
| **latency** | p99 of `occurred_at` → oracle receipt ≤ `P99_TARGET_MS` | Also reports p50, p95, max |
| **no late delivery** | nothing arrives after its `expires_at` | OTP TTL guardrail |

A check reports SKIP when it has no data yet (for example, no expected events registered).

### Duplicates, the realistic way
Real providers remember idempotency keys **per provider**. The oracle does the same:
- **Same provider, same `Idempotency-Key`** (the worker retried after a lost ack): absorbed. The reply is 200 with the same `provider_message_id`; it is counted as an `idempotent replay`, not a duplicate.
- **Different provider** (the worker failed over even though primary had delivered): a second text on the customer's phone, so **no duplicates** fails. This is the bug the failover tests are meant to catch.

Set `HONOR_IDEMPOTENCY=false` to act as a naive provider, where every call is a new text. This is useful for demoing why the key matters.

## API

**Provider side** (called by the adapters):

| | |
|---|---|
| `POST /sms`, `/push`, `/email` | JSON `{event_id, account_id, sequence, occurred_at, provider, ...}`, header `Idempotency-Key`. Returns 200, or 503/422 when a fault rule applies. Phone numbers and message text are never stored or logged. |

**Harness side** (called by the load generator and chaos scripts):

| | |
|---|---|
| `POST /admin/reset` | Start a new run; clears everything |
| `POST /expected` | Register events the producers sent: one object or a list. Accepts snake_case or proto-JSON camelCase, so raw `NotificationRequested` JSON works. |
| `GET /report` | JSON verdict. `?format=text` gives the table for `docs/results/`. |
| `GET /deliveries?account_id=` | What reached the phone, in arrival order |
| `POST /admin/rules` | Per-event faults: `{"event_id": "...", "action": "fail", "times": 2}` → 503 twice then accept (T4). `{"event_id": "...", "action": "reject"}` → 422 forever (T5 poison). |
| `GET /admin/rules`, `DELETE /admin/rules` | List or clear rules |
| `GET /health` | Liveness plus counts |

`/admin/*` and `/expected` require `Authorization: Bearer $ADMIN_TOKEN` when `ADMIN_TOKEN` is set. Provider endpoints stay open for the adapters.

## Example report
From the Nov 9 end-to-end check: retries, a lost-ack replay, a poison message and an OTP sent through Toxiproxy. The latency line shows the format only; real numbers will come from the load generator.
```
CHECK                RESULT  DETAIL
-------------------- ------- --------------------------------------------------
no loss              PASS    6 expected: 5 delivered, 1 dead-lettered, 0 missing
no duplicates        PASS    5 unique events, 0 delivered more than once
per-account order    PASS    5 deliveries across 1 accounts, 0 out of order
latency              PASS    p50 <n> ms, p99 <n> ms (target 2000 ms)
no late delivery     PASS    1 deliveries with a TTL, 0 arrived after expiry

provider calls: 10 total (sms_primary 8, sms_secondary 2)
  delivered 5, failed by rule 2, idempotent replay 1, rejected by rule 2
ledger: connected (1 expired/dead-lettered)
```

## Configuration
| Env var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | *(unset)* | Postgres, to read expired/dead-lettered events. Unset means those count as missing. |
| `P99_TARGET_MS` | `2000` | Latency target |
| `HONOR_IDEMPOTENCY` | `true` | Act like a real provider (see above) |
| `ADMIN_TOKEN` | *(unset)* | Protects the harness endpoints |

State is kept in memory on purpose. The oracle is test equipment, and each run starts with a reset.

## Run and test
```bash
docker compose up -d --build oracle sms-adapter-primary sms-adapter-secondary toxiproxy postgres
curl localhost:18080/report?format=text

# unit tests (23)
docker build --target test -t oracle:test harness/oracle && docker run --rm oracle:test
```
