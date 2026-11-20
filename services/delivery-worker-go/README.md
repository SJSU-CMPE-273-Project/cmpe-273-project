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
contiguous sequences: ingest rejection can leave gaps. A late sequence at or
below the high-water mark is ignored. Temporal history must be retained; do
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
retry timers are bounded by the remaining TTL. An exhausted definitive failure
publishes a Protobuf `DeadLetter` to `notifications.dlq`, keyed by account ID.
Publishing can replay after a lost acknowledgement: DLQ readers must deduplicate
by original event ID.

## Run

```sh
docker compose up -d --build delivery-worker oracle postgres
```

The service uses `TEMPORAL_ADDRESS=temporal:7233`,
`TEMPORAL_NAMESPACE=relayguard`, and `KAFKA_BROKERS=kafka:9092` in Compose.
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

The broader Lane 2 design also requires a Postgres delivery ledger with an
atomic `deliveries.event_id` claim, delivery/attempt/status records, configured
guardrail settings, shared retry budgets, and durable reconciliation of uncertain
sends. Those are not implemented by this worker yet. In particular, uncertain
sends without an expiry can block that account indefinitely and grow history;
operator reconciliation and continuation during a long retry are still needed.
Expiration currently ends workflow processing without writing an `EXPIRED`
ledger/status record.
The ingest bridge can publish the same event twice after an acknowledgement/
ledger crash. Workflow deduplication covers normal Kafka replay and worker
restarts, including Continue-As-New, but does not replace the durable database
claim required for reset/reconciliation workflows. Do not claim full T2
(crash/at-most-once), T4 (ordering), T5 (poison), or T6 (provider outage) chaos
acceptance until those scenarios run against the complete platform and ledger.
