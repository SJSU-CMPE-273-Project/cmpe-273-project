# RelayGuard Ops Console (Lane 4)

**Owner:** Katherine · **Stack:** React, Express, PostgreSQL · **Public surface:** port `18083`

## What it includes

- Live delivery metrics: delivered, retrying, dead letters, ingest rejects and p99 latency
- Account delivery timeline with state, channel, provider and attempt count
- DLQ list with attempt history and replay guardrails
- Producer kill switch and rate-limit controls
- Agent proposals with prompt versioning and mandatory human approval
- Audit records for replay, agent and guardrail actions

## Run locally

```bash
docker compose up -d postgres
docker compose up -d --build ops-console
```

Open `http://localhost:18083`.
Set `OPERATOR_TOKEN` to require Bearer authentication:

```bash
OPERATOR_TOKEN=change-me docker compose up -d ops-console
```

The UI sends `X-RelayGuard-Operator`; set a stable operator name in the console header.

## Replay contract

Replay is intentionally approval-only until Lane 2 consumes `replay_requests`:

- The event must exist in `dead_letters`.
- Already replayed events are refused.
- Expired events are refused.
- The 24-hour replay cap is enforced.
- The replay request retains the original `event_id` as the idempotency key.
- Approval and refusal are recorded in `ops_audit`.

Lane 2 is responsible for executing an approved row and must set `replayed_at` and
`replayed_by` only after a workflow has started successfully.

## API

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/health` | Health check |
| GET | `/api/metrics` | Console metrics |
| GET | `/api/deliveries?account=` | Delivery timeline |
| GET | `/api/deliveries/:eventId` | Delivery plus attempts |
| GET | `/api/dlq` | DLQ with attempt history |
| POST | `/api/dlq/:eventId/replay` | Create replay approval intent |
| GET | `/api/controls` | Producer and guardrail settings |
| PUT | `/api/controls/producers/:producer` | Kill switch / rate limit |
| PUT | `/api/controls/guardrails/:key` | Guardrail setting |
| GET | `/api/agent/proposals` | Existing proposals |
| POST | `/api/agent/evaluate/:eventId` | Create masked proposal |
| POST | `/api/agent/proposals/:id/approve` | Human approval |

## Agent harness

`agent.js` loads `agent/prompts/${AGENT_PROMPT_VERSION}.md` (default `dlq-v1`)
as the system prompt and calls the OpenAI Chat Completions API with a strict
JSON schema. `OPENAI_API_KEY` is server-only. `AGENT_MODEL` defaults to
`gpt-4.1-mini`. The request contains only priority, normalized reason, expired
boolean and up to 20 attempts (allowlisted provider, code, ISO time). Bodies,
phone numbers, account numbers and arbitrary log text are excluded. Unknown
reason/provider/code strings become `unknown`/`other`; no recovery is inferred
from a free-text provider error. `poison_after_fix` is a fixture reason; real
`poison` rows remain ambiguous to the model until recovery evidence is supplied.

The model returns exactly `{action, rationale, risk}`. Invalid JSON, extra keys,
invalid enums, empty/oversized rationale, refusal, truncation, missing usage, or
an expired replay recommendation produce `escalate`. Model outages, missing
credentials and budget exhaustion use the original rules (expired → discard;
poison/rejection → replay conditional on a fix; otherwise escalate). Every
proposal records prompt version, configured model, token counts, risk, source,
estimated cost and whether usage is uncertain. Fallbacks never approve a replay.

`GET /api/agent/usage` shows today's UTC input/output tokens, estimated cost,
budget consumed/reserved and configured daily limit. The UI shows these totals
and each proposal's provenance. A replay proposal's Approve button records human
approval then opens the existing explicit replay confirmation. That confirmation
uses `/api/dlq/:eventId/replay`, the Issue B boundary. This branch still records
`replay_requests` intents; executing them belongs to Issue B. The agent never
calls the replay endpoint or a delivery provider.

## Configuration and existing databases

| Variable | Default |
|---|---|
| `AGENT_PROMPT_VERSION` | `dlq-v1` |
| `AGENT_MODEL` | `gpt-4.1-mini` |
| `OPENAI_API_KEY` | empty → rules fallback |
| `AGENT_DAILY_TOKEN_BUDGET` | `100000` |
| `AGENT_INPUT_USD_PER_MILLION` | `0.40` |
| `AGENT_OUTPUT_USD_PER_MILLION` | `1.60` |

Set both prices when changing models. Prompt files are copied through Compose's
`prompts` additional build context; rebuild the console after prompt changes.
For an existing database, apply these additive migrations without deleting data:

```bash
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/005_ops_console.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/006_agent_harness.sql
docker compose up -d --build ops-console
```

The budget is shared by all console instances. Before any model call, PostgreSQL
atomically reserves a conservative UTF-8 byte bound plus schema/framing allowance
and the 256-token output cap. Known usage settles to provider counts. Timeouts,
errors, crashes and missing usage keep the reservation; timeout/error costs use
a conservative bound. A crash before settlement can leave cost unrecorded but
cannot release budget. Daily accounting is independent of proposal insertion,
so an insert failure does not erase model spending. Requests never retry.

## Evaluation and token economics

```bash
npm ci
npm test
npm run build
# Real provider run; fails if OPENAI_API_KEY is absent or any fallback occurs:
AGENT_EVAL_REPORT=../../agent/evals/live-results.json npm run eval:live
# Reproduce zero-call baseline and conservative projections:
node ../../agent/evals/economics.js
```

The fixed eval set covers expired OTP, poison after fix, provider outage and
body injection. Offline contract tests mock the provider; they verify harness
behavior, not model classification quality. CI runs them and database integration
checks on every harness/prompt change, with live evals on trusted pushes/manual
runs using repository secret `OPENAI_API_KEY`. Fork PRs never receive that secret.
The live job fails if the secret is missing; it cannot report a false pass.

For local database integration tests, create a fresh disposable database named
`relayguard_agent_test` and set `AGENT_TEST_DATABASE_URL` when running `npm test`.
The test applies migrations, verifies concurrent reservations, exercises proposal
creation/listing/usage, and verifies no replay intent exists until an explicit
human replay request. Without the URL this integration test is skipped.

The committed [baseline](../../agent/evals/economics-baseline.json) has 4 scenarios,
0 external calls, 0 measured tokens and $0 measured spend. Fixture reservation
bounds are 5,888–5,997 tokens; the largest fixture projects at most $2.706 per
1,000 items at default rates. Live model measurements remain pending credentials.
Do not present mocked counts or conservative bounds as measured model economics.

## Security

- Operator authentication uses the existing `OPERATOR_TOKEN` Bearer check.
- Parameterized SQL, a 32 KB JSON limit, and a non-root, read-only container.
- PII exclusion and closed vocabularies before the model boundary (OWASP LLM02).
- Untrusted-data system instructions and strict response validation (LLM01/LLM05).
- No model tools or execution permissions; human approval and replay guards (LLM06).
- Bounded inputs, output cap, daily reservations and timeout (LLM10).
- Model errors log no prompts, responses, credentials or provider error bodies.
