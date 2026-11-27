# RelayGuard Ops Console (Lane 4)

**Owner:** Katherine · **Stack:** React, Express, PostgreSQL · **Local surface:** `localhost:18083`

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
POSTGRES_HOST_PORT=5434 docker compose up -d --build postgres ops-console demo-trigger
```

Open `http://localhost:18083`.
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

The initial implementation is a deterministic, auditable policy engine. Its versioned
prompt is at `agent/prompts/dlq-v1.md`. It uses no raw message body and creates a
proposal rather than executing an action.

Rules:
- expired message → discard
- provider rejection / poison → replay after provider fix
- provider outage or unclear failure → escalate
- every proposal requires human approval
- only `replay_requests` is written for replay approval; no provider call is made

Before a hosted LLM is enabled, configure a provider adapter, send masked summaries
instead of raw logs, enforce a hard token budget, and record input/output counts.

## Token economics

The hosted agent is currently disabled, so there is no external token cost.

| Item | Measured / planned |
|---|---|
| External LLM calls | 0 (deterministic policy engine) |
| Cost per 1,000 items | $0 until hosted LLM is enabled |
| Hard daily replay cap | `REPLAY_DAILY_CAP`, default 20 |
| Prompt version | `AGENT_PROMPT_VERSION`, default `dlq-v1` |

## Security

- Bearer token on mutating/read APIs when `OPERATOR_TOKEN` is set
- Operator identity in audit records
- Parameterized PostgreSQL queries
- JSON body limit of 32 KB
- PII not returned or sent to the current policy engine
- Replay, expiration and daily replay guardrails
- Static React bundle; no privileged server-side tool calls
- Container runs as non-root and uses a read-only filesystem

## Tests

```bash
npm test
npm run build
docker build -t relayguard-ops-console:test .
```
