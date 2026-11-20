# Delivery Worker (Go, Temporal)

## Requirements checklist

1. **One entity workflow per `account_id`.** The Kafka consumer calls
   `SignalWithStartWorkflow` with the literal account ID as workflow ID in
   namespace `relayguard`, task queue `delivery`. Each workflow waits for one
   delivery to finish before starting the next. Accounts run independently.
2. **Adapters through Toxiproxy.** HTTP activities only use
   `http://toxiproxy:21001/send` (SMS primary), `:21002/send` (SMS secondary),
   and `:21003/send` (push/email). Redirects are disabled. Direct adapter host
   ports are for debugging only.
3. **Headers.** Every send sets `Idempotency-Key` to the original `event_id`
   and forwards the original `traceparent`, including retries and failover.
4. **Adapter contract.** See [SMS HTTP contract](../sms-adapter-ruby/README.md)
   and [push/email additions](../push-email-adapter-node/README.md). Activities
   send the required JSON and account/sequence/occurrence metadata. HTTP 200
   accepted completes delivery, 400 dead-letters immediately, 422 advances to
   the next target, 429 waits for `Retry-After` without consuming the failure
   budget or increasing backoff, and 503 retries with exponential
   backoff plus jitter before failover. Each target gets at most five failures
   (a circuit opening can cut that short). Requested channel
   order is preserved; SMS expands to primary then secondary.

## Ordering and recovery

The consumer reads raw Protobuf `NotificationRequested` records from
`notifications.requested`, validates the contract and account key, and commits
Kafka offsets synchronously **after** Temporal accepts the signal. Failed
handoffs stop the consumer without committing that record; Compose restarts it.
Malformed records go to `notifications.dlq` as Protobuf `DeadLetter` messages
with reason `invalid_kafka_record` before being committed.

The bridge must publish records for each account in increasing sequence order
and keep Kafka partitioning stable. The workflow sorts buffered signals by
sequence and ignores duplicate event IDs/sequences. It retains a sequence
high-water mark across Continue-As-New (every 1,000 processed messages), draining
signals and carrying the pending queue into the next run. It does not wait for
contiguous sequences: ingest rejection can leave gaps. A normal Kafka record at or below the high-water mark is ignored. Operator
replay uses the separate workflow described below. Temporal history must be retained; do
not terminate/reset account workflows as an ordinary replay mechanism.

HTTP 504, transport errors after connection, malformed responses, and a retried activity following
a worker crash are ambiguous: the provider may already have accepted the send.
The workflow pins subsequent retries to that provider and key, even if a later
attempt gets a permanent rejection. It does not fail over an uncertain send.
This relies on the provider retaining idempotency keys across its own restarts.
A received 503 is treated as a definitive temporary failure per the adapter
contract; providers must not accept a send and then return 503. Dial failures
are definitive because no request was sent, so they can retry and fail over.
503/504/422 status classification does not require a JSON response body. A 422
never retries that target unless an earlier ambiguous send still needs reconciliation.

Retry timers use equal jitter: a random delay in `[base/2, base]`, with bases
of 1, 2, 4, 8, 16, then 32 seconds (capped). Temporal records each random draw
using `SideEffect` and preserves the previous timer behavior when replaying old
histories through a version marker. `Retry-After` is a minimum delay and TTL
bounds the timer. HTTP 429 uses its `Retry-After` delay directly, without
jitter or exponential backoff; missing/invalid headers default to one second.
Both delay-seconds and HTTP-date headers are supported. Repeated throttling
keeps the current provider until it accepts, fails, or the message expires.

Each provider has an independent circuit shared across accounts in the worker
process. Three consecutive availability failures (503, 504, transport errors,
or invalid responses) open it for 30 seconds. Open circuits skip new sends to
the next requested target. After cooldown, exactly one half-open probe is
allowed: success or a definitive rejection closes the circuit; another failure
reopens it. 400/422/429 do not count as availability failures. A 429 preserves
prior failure counts and releases a half-open probe without closing the circuit.
An uncertain send waits for cooldown and probes the same provider with its original key; it never
fails over solely because the circuit opened. Breaker state is in memory and
resets on worker restart; multiple worker processes have independent breakers.
A cluster-wide retry budget and shared durable breaker state remain future work.

Expiry is checked before each attempt in both the workflow and activity, and
retry timers are bounded by the remaining TTL. Both paths persist
`deliveries.state = expired`. Exhausted definitive failures atomically persist
`dead_lettered` and the original Protobuf payload in `dead_letters`, then publish
a Protobuf `DeadLetter` to `notifications.dlq`, keyed by account ID. Its attempts
come from the complete, ordered Postgres audit history. A publication retry uses
the stored dead-letter timestamp and reason. DLQ readers must deduplicate by
original event ID because a lost Kafka acknowledgement may cause republication.

Postgres is required. The worker creates delivery rows only for events already
present in `ingested_events`; ingest remains the admission authority. Each send
holds a session advisory lock for its event across the HTTP call. Attempt
reservations commit before HTTP; interrupted sends remain visible as
`interrupted_or_pending`. Completed attempts store channel, provider, time,
latency, success and a PII-free error classification. A delivered ledger row
skips subsequent sends across all providers. Provider idempotency with the same
`event_id` protects the gap between provider acceptance and ledger commit.

## DLQ and operator replay (T5)

Apply the migration to an existing Compose database before restarting workers
(fresh databases apply it automatically):

```sh
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < infra/postgres/init/005_delivery_replay.sql
docker compose up -d --build delivery-worker oracle
```

After fixing the provider rejection, start an audited replay with:

```sh
docker compose exec delivery-worker /delivery-worker replay EVENT_UUID OPERATOR_NAME
```

This starts `delivery-replay/EVENT_UUID` on the delivery task queue; it does not
wait for completion. Inspect that workflow in Temporal and the delivery ledger
for the outcome. Repeated/concurrent operator requests are rejected by Temporal's
workflow ID policies and a durable database claim tied to the replay run. Activity
retries of the same claim remain safe. Exactly one replay is allowed per event;
a failed replay returns to DLQ and retains all previous attempts. It cannot be
claimed again as a new replay run.

Replay loads the stored original payload and preserves `event_id`, account,
sequence, channels and TTL. It bypasses the account sequence high-water mark;
it does not change ordinary Kafka deduplication. Expired notifications remain
expired. Original and replay attempts have one monotonically increasing audit
ordinal. Old DLQ publication retries cannot overwrite a claimed replay. Replay
metadata (`replayed_at`, `replayed_by`, `replay_token`) and the original DLQ row
remain after success. The oracle reads terminal states from `deliveries` and
uses historical `dead_letters` rows to retain the late-replay ordering exemption.

The stored original and Kafka DLQ payload contain recipient/body data needed
for replay; attempt history contains only safe error classifications. Restrict
access to the database, DLQ topic and operator command accordingly.

Run database-backed workflow, retry, concurrency and crash-gap checks using an
isolated test schema (the database role needs `CREATE SCHEMA`):

```sh
DELIVERY_TEST_DATABASE_URL=postgresql://... go test -race ./services/delivery-worker-go/...
ORACLE_TEST_DATABASE_URL=postgresql://... python -m pytest harness/oracle/tests
```

These tests cover max retries with complete history, permanent poison rejection,
publication acknowledgement loss, a single replay with the original key,
concurrent replay claims, duplicate activities, provider acceptance followed by
ledger completion loss, and expiry before send/during backoff.

## Run

```sh
docker compose up -d --build delivery-worker oracle postgres
```

The service uses `TEMPORAL_ADDRESS=temporal:7233`,
`TEMPORAL_NAMESPACE=relayguard`, `KAFKA_BROKERS=kafka:9092`, and the shared
`DATABASE_URL` in Compose.
The Compose service and Kafka consumer group are both named `delivery-worker`,
matching the chaos scripts; no `WORKER_SERVICE` or `WORKER_GROUP` overrides
are needed.

For a host process, Temporal defaults to `localhost:7233`, Kafka to
`localhost:9094`. Adapter traffic still requires the `toxiproxy` hostname and
ports to be reachable; use Compose for end-to-end execution.

```sh
go test ./services/delivery-worker-go/...
go test ./...
```

The Compose integration test uses unique synthetic accounts and event-scoped
Oracle fault rules. It does not reset existing Oracle results or change global
adapter fault modes.

```sh
DELIVERY_INTEGRATION=1 go test -count=1 -v -run TestComposeDelivery ./services/delivery-worker-go
```

Verified against Docker on November 20, 2026: six events delivered once across
five accounts, serial account ordering, independent-account progress during
retries, and primary/secondary/push/email routes. Both HTTP headers pass the
request-level tests. The live Oracle report is saved in
[`docs/results/delivery-worker-compose.json`](../../docs/results/delivery-worker-compose.json).
Its latency check exceeds the default two-second target because this scenario
intentionally injects two failures and waits through retry backoff. This run is
not a latency benchmark or full chaos acceptance.

Temporal's named SQLite volume is initialized by `temporal-data-init` so the
server's non-root user can write the database on a fresh Docker volume.

Tests cover routing, headers, JSON payloads, response classification,
back pressure, failover, ambiguous retries, account isolation, duplicate signals
(including during a send), sequence ordering, and continuation state.

## Retry and failover verification

```sh
go test -race ./services/delivery-worker-go/...
go test -v -run 'TestT6ProviderOutageFailover|TestRetryJitterBoundsAndAmbiguousPinning|TestProviderCircuitLifecycle' ./services/delivery-worker-go
```

The T6 workflow/HTTP transport test injects provider 503 outages, asserts that
circuits stop outgoing calls after three failures, and measures virtual workflow
time to secondary SMS, push, and email. Other tests cover refused connections,
empty/non-JSON 422/503/504 responses, cooldown and probe recovery, jitter bounds,
`Retry-After`, and retry pinning after ambiguous sends. These are automated
component tests; a live Compose/Toxiproxy T6 run remains a separate acceptance check.

## Remaining delivery milestones

Remaining work includes published delivery status events, configured guardrail
settings, shared retry budgets, and durable reconciliation of uncertain sends.
Uncertain sends without an expiry can still block an account indefinitely and
grow workflow history; operator reconciliation and continuation during long
retries remain separate work. Provider idempotency retention is required for
crash recovery. The database-backed tests use a real Postgres ledger and Temporal's
workflow test environment; full live Compose/Toxiproxy chaos acceptance is a
separate check.
