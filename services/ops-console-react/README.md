# RelayGuard Ops Console (Lane 4)

**Owner:** Katherine · **Stack:** React, Express, PostgreSQL · **Local surface:** `localhost:18084`

## What it includes

- Live delivery metrics: delivered, retrying, dead letters, ingest rejects and p99 latency
- Account delivery timeline with state, channel, provider and attempt count
- DLQ list with attempt history and replay guardrails
- Producer kill switch and rate-limit controls
- Agent proposals with prompt versioning and mandatory human approval
- Audit records for replay, agent and guardrail actions
- Public demo mode with a bounded smoke trigger and read-only Oracle results

## Run locally

```bash
POSTGRES_HOST_PORT=5434 docker compose up -d --build postgres delivery-worker ops-console demo-trigger
```

Open `http://localhost:18084`. The push/email adapter uses port `18083`;
set `OPS_CONSOLE_HOST_PORT` to choose another console port.
Set `OPERATOR_TOKEN` to require Bearer authentication:

```bash
OPERATOR_TOKEN=change-me docker compose up -d ops-console
```

The UI sends `X-RelayGuard-Operator`; set a stable operator name in the console header.
To wipe data and initialize the schema from scratch, run `make reset` before the
startup command (it deletes all Compose volumes).

## Public demo

The no-login public console is read-only except for a fixed demo trigger. Enable it
with `PUBLIC_DEMO=true`; privileged replay, producer-control and agent-write API
routes return 403. The page shows live Postgres-backed metrics and a five-second
refresh, plus three buttons: refresh, run a fixed three-event smoke test (one event
per second, 30-second cooldown), and open the Oracle report. The local Compose demo
targets the adapter path; k3s config targets Iggy and therefore requires the Lane 2
worker. The adapter-only mode does not prove worker delivery or Kafka balancing.

On a droplet, use the k3s path in [deploy/k3s/README.md](../../deploy/k3s/README.md)
or the Caddy fallback by setting `DOMAIN` and running Compose with
`docker-compose.droplet.yml`. The Caddy config obtains HTTPS certificates
automatically. Do not expose app or broker ports in the droplet firewall.

## Replay contract

Human-confirmed replay is queued in `replay_requests` and consumed by the delivery worker:

- The event must exist in `dead_letters`.
- Already replayed events are refused.
- Expired events are refused.
- The 24-hour replay cap is enforced.
- The replay request retains the original `event_id` as the idempotency key.
- Approval and refusal are recorded in `ops_audit`.

The worker polls committed approvals once per second, locks a request with
`FOR UPDATE SKIP LOCKED`, and calls Issue B's `StartReplay()`. It records
`started` plus `workflow_id` and `started_at` after Temporal accepts the workflow.
`started` means submitted, not delivered; the timeline's delivery state shows the
outcome. Invalid or expired requests become `refused` with a safe reason. Temporary
failures remain `approved` and retry after a bounded attempt and backoff.

If Temporal accepts the replay but the database update fails, the next dispatch
reconciles `delivery-replay/<event_id>` before checking current TTL or ledger state.
A concurrent manual submission is also reconciled. The fixed workflow ID prevents
another replay run. Dispatch state and its reason appear on DLQ rows and proposals.
No Docker socket, subprocess, or provider-send permission is given to the console.

## API

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/health` | Health check |
| GET | `/api/results` | Read-only Oracle report |
| GET | `/api/demo/status` | Safe demo trigger status/cooldown |
| POST | `/api/demo/run` | Fixed bounded smoke test; no user-controlled input |
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

`agent.js` loads `agent/prompts/${AGENT_PROMPT_VERSION}.md` (default `dlq-v2`)
as the system prompt and calls Gemini through Google's OpenAI-compatible Chat
Completions endpoint with a strict JSON schema. `GEMINI_API_KEY` is server-only.
`AGENT_PROVIDER` defaults to `gemini`; `AGENT_MODEL` defaults to
`gemini-3.5-flash-lite`. OpenAI remains available with `AGENT_PROVIDER=openai`
and `OPENAI_API_KEY` (default model `gpt-4.1-mini`). The request contains only priority, normalized reason, expired
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
and each proposal's provenance. A replay proposal's Approve button asks for human
confirmation before making any change. The approval endpoint atomically marks the
proposal approved, enqueues the replay with the proposal's original prompt version,
and writes audit records. Cancelling confirmation leaves it undecided. Expiry and
24-hour replay limits use the same transaction helper as direct operator replay.
The agent never calls approval, replay, or a delivery provider.

## Configuration and existing databases

| Variable | Default |
|---|---|
| `AGENT_PROMPT_VERSION` | `dlq-v2` |
| `AGENT_PROVIDER` | `gemini` (`openai` also supported) |
| `AGENT_MODEL` | `gemini-3.5-flash-lite` for Gemini; `gpt-4.1-mini` for OpenAI |
| `GEMINI_API_KEY` | empty → rules fallback for Gemini |
| `OPENAI_API_KEY` | used only when provider is `openai` |
| `AGENT_DAILY_TOKEN_BUDGET` | `100000` |
| `AGENT_INPUT_USD_PER_MILLION` | `0` for Gemini free tier; `0.40` for OpenAI |
| `AGENT_OUTPUT_USD_PER_MILLION` | `0` for Gemini free tier; `1.60` for OpenAI |

Set both prices when changing models or using a paid Gemini project.
These rates control accounting only; they do not change Google billing.
Google lists free-tier quotas and data-use terms on its
[pricing page](https://ai.google.dev/gemini-api/docs/pricing).
The free tier may use content to improve Google products; only allowlisted summaries
leave this service. A 429/quota error uses the existing rules fallback.

Create a key in [Google AI Studio](https://aistudio.google.com/apikey).
From a zsh terminal at the repository root, load it without shell-history exposure:

```zsh
read -rs 'GEMINI_API_KEY?Paste your Gemini API key: '
echo
export GEMINI_API_KEY
AGENT_EVAL_REPORT=agent/evals/live-results.json node agent/evals/run.js
docker compose up -d --build ops-console
```

Keep the key out of Git, browser code and chat. For GitHub Actions, add a
repository Actions secret named `GEMINI_API_KEY`.

Prompt files are copied through Compose's
`prompts` additional build context; rebuild the console after prompt changes.
For an existing database, apply these additive migrations without deleting data:

```bash
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/005_ops_console.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/006_agent_harness.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/005_delivery_replay.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/007_replay_dispatch.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/008_dlq_discard.sql
docker compose up -d --build delivery-worker ops-console
```

The budget is shared by all console instances. Before any model call, PostgreSQL
atomically reserves a conservative UTF-8 byte bound plus schema/framing allowance
and the output cap (1,024 tokens for Gemini, including reasoning; 256 for OpenAI).
Gemini uses `reasoning_effort=minimal`; truncated output still fails closed.
Known usage settles to provider counts. Timeouts,
errors, crashes and missing usage keep the reservation; timeout/error costs use
a conservative bound. A crash before settlement can leave cost unrecorded but
cannot release budget. Daily accounting is independent of proposal insertion,
so an insert failure does not erase model spending. Requests never retry.

## Evaluation and token economics

```bash
npm ci
npm test
npm run build
# Real provider run; fails if the selected provider key is absent or any fallback occurs:
AGENT_EVAL_REPORT=../../agent/evals/live-results.json npm run eval:live
# Reproduce zero-call baseline and conservative projections:
node ../../agent/evals/economics.js
```

The fixed eval set covers expired OTP, poison after fix, provider outage and
body injection. Offline contract tests mock the provider; they verify harness
behavior, not model classification quality. CI runs them and database integration
checks on every harness/prompt change, with live evals on trusted pushes/manual
runs using repository secret `GEMINI_API_KEY`. Fork PRs never receive that secret.
The live job fails if the secret is missing; it cannot report a false pass.

For local database integration tests, create a fresh disposable database named
`relayguard_agent_test` and set `AGENT_TEST_DATABASE_URL` when running `npm test`.
The test applies migrations, verifies concurrent reservations, exercises proposal
creation/listing/usage, and verifies no replay intent exists until human approval, then checks atomic
queueing, duplicate refusal, expiry, prompt provenance, and concurrent caps. Without the URL this integration test is skipped.

The [passing v2 live run](../../agent/evals/results/dlq-v2-gemini-2026-11-27.json)
measured 2,120 input and 112 output tokens across four fixtures: 558 total tokens
per item (530 input + 28 output). All four expected actions passed with
`source=model`; estimated cost is $0 at configured free-tier rates. This is one
small eval run, not a production quality or volume estimate.

The [first v1 run](../../agent/evals/results/dlq-v1-gemini-2026-11-27.json)
passed 3/4: the model confused `channels_exhausted` with resource exhaustion and
escalated the expired OTP. `dlq-v2` adds explicit ordered rules and expiry examples.
The console and CI now default to v2; v1 remains available for comparison.

Re-run the active prompt from the repository root:

```zsh
AGENT_EVAL_REPORT=agent/evals/live-results-v2.json node agent/evals/run.js
```

A passing eval requires `source=model` and the expected action for all four
cases. Fallbacks or postprocessing cannot turn a model failure into a passing
eval. The committed [offline baseline](../../agent/evals/economics-baseline.json)
records zero external calls; reservation bounds are separate projections.

## Troubleshooting live eval failures

`source: model_unavailable` means the rules fallback ran, not that the model eval
passed. Live results now include safe `model_error` diagnostics: HTTP status,
closed error category, recognized provider codes, or recognized network codes.
Raw error messages, prompts and API keys are never included.

From the same terminal where you exported the key:

```bash
node agent/evals/models.js
AGENT_EVAL_REPORT=agent/evals/live-results.json node agent/evals/run.js
```

The first command lists Gemini text-generation models visible to your key using
Google's [models endpoint](https://ai.google.dev/api/models). Listing a model does
not establish free-tier quota or JSON schema support. Use `AGENT_MODEL` to select
an available compatible model after checking its capabilities. A `404` indicates
a missing model/resource, `401`/`403` points to authentication or access, `429`
indicates quota/rate limits, and `400` indicates an invalid request (including
some key/configuration errors). Do not infer a specific cause from the fallback
alone. Share only `model_error` and model names when requesting help.

## Security

- Operator authentication uses the existing `OPERATOR_TOKEN` Bearer check (constant-time compare).
- With no `OPERATOR_TOKEN` and `PUBLIC_DEMO` off, write routes accept only loopback or
  private-network clients with no `X-Forwarded-For`/`Forwarded` header. Local Compose works
  as before; a public deploy behind a proxy gets `403` until one of the two is set.
- Approving a `discard` proposal sets `dead_letters.discarded_at`/`discarded_by` (migration
  008): the item leaves the DLQ list and any later replay is refused with `409`.
- With both price variables at `0` (Gemini free tier) the console shows "free tier, no charge"
  instead of a `$0` estimate.
- Parameterized SQL, a 32 KB JSON limit, and a non-root, read-only container.
- PII exclusion and closed vocabularies before the model boundary (OWASP LLM02).
- Untrusted-data system instructions and strict response validation (LLM01/LLM05).
- No model tools or execution permissions; human approval and replay guards (LLM06).
- Bounded inputs, output cap, daily reservations and timeout (LLM10).
- Model errors log no prompts, responses, credentials or provider error bodies.
