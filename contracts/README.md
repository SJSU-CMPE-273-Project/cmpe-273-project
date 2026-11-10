# NotificationRequested v1 contract review

Status: implemented proposal; **Aaron (Lane 2) acceptance is pending**.
Aaron owns the Lane 2 delivery feature and has not started its consumer yet.
Before consumer implementation, Aaron should accept the exact raw Protobuf
value / UTF-8 account key / no Confluent framing proposal below. Record his
acceptance here; it has not been obtained in this task.

`proto/relayguard/v1/notification.proto` is the source of truth. Generated code
is checked in under `gen/{java,python,ruby,go}`; never hand-edit those files.
The JSON Schema narrows Protobuf to the application profile below.

## Wire format

| Boundary | Encoding | Key / routing |
|---|---|---|
| Producer → Iggy `relayguard/requests` | UTF-8 ProtoJSON, generated serializer | Explicit partition ID: first 4 bytes of SHA-256(UTF-8 account_id), interpreted unsigned big-endian, modulo **6** |
| Bridge → Kafka `notifications.requested` | Raw binary `relayguard.v1.NotificationRequested` | Key: exact UTF-8 `account_id` bytes |

Kafka has **no** Confluent magic byte, schema ID, message-index prefix, JSON
envelope, base64 wrapper, or length prefix. Decode the complete record value
with the generated `NotificationRequested` binding. Standard Protobuf wire
compatibility rules apply: do not reuse tags or enum numbers. Unknown future
fields are handled by the generated binary decoder. Headers are optional and
must not be required for decoding. The bridge's tested codec is
`contracts.Decode` → `contracts.KafkaRecord`; the TCP probe also exercises it.
This step does not implement Kafka forwarding or delivery processing.

ProtoJSON uses lowerCamelCase field names, symbolic enum names, UTC `Z`
timestamps with 0/3/6/9 fractional digits, decimal-string `sequence`, ordered
channel arrays, and omitted unset optional fields. Canonical here means the
Protobuf JSON mapping/profile, not byte-identical JSON or sorted object keys.
Validation asserts date-time and UUID formats, rejects unknown fields and
unspecified enums, and rejects duplicate object keys before decoding.

## Sequence range and ownership

Keep `uint64` on the wire for compatibility, with an application range of
**1..9223372036854775807**. JSON Schema uses a decimal regex to enforce the exact
range without rounding; Go and all three producer codecs also check it.
PostgreSQL `BIGINT` supplies the upper bound and `CHECK (sequence > 0)` supplies
the lower bound. Values beyond that range are rejected, never wrapped or
truncated. Exhausting an account's sequence stops that producer.

Fresh databases load both init SQL files. For an existing volume, apply
`infra/postgres/init/002_sequence_range.sql` with `psql -v ON_ERROR_STOP=1`.
It validates existing data and is safe to re-run. It does not erase data.

| Sole owner | Default account pool | Stable state |
|---|---|---|
| Java banking | `bank-demo-0001` … `bank-demo-9999` | `.state/java.sqlite3` |
| Python fraud | `fraud-demo-0001` … `fraud-demo-9999` | `.state/python.sqlite3` |
| Ruby OTP | `otp-demo-0001` … `otp-demo-9999` | `.state/ruby.sqlite3` |

Run `tools/demo-producer.py` as the owner; the language programs are one-shot
codec/transport helpers. The runner holds an exclusive OS lock through publish,
persists sequence + exact event in one SQLite FULL-synchronous transaction,
then publishes one event at a time. It marks that event acknowledged only after
the transport returns success. A failure stops publication; the next process
replays pending bytes/ID/sequence before allocating another sequence. A crash
after broker acknowledgement can therefore duplicate an event, but cannot
allow a later new event to overtake it. Downstream dedup uses `event_id`.

All instances for one pool must share the same durable state directory on one
host. Do not delete or fork that state, run the helpers as independent synthetic
producers, change the six Iggy partitions, or change account routing mid-demo.
The smoke suite owns separate `*-smoke-*` pools and retained state. `--dry-run`
requires a separate state directory and cannot later be used in live mode.
Shared-account traffic remains disabled. Before enabling it, agree on one
central durable sequencer/outbox and one serialized publisher per account;
independent incrementers plus a Kafka key cannot establish business ordering.
Kafka's key preserves broker arrival order within its partition only. Keep
Kafka topic partition counts fixed during the demo too.

## Toolchain and running

The [official compatibility matrix](https://iggy.apache.org/docs/sdk/introduction/)
pins server 0.9.0 to Java `org.apache.iggy:iggy:0.9.0`, Python
`apache-iggy==0.9.0`, and Go `github.com/apache/iggy/foreign/go v0.9.0`.
Ruby uses the [tagged 0.9.0 HTTP API](https://github.com/apache/iggy/tree/server-0.9.0/core/common/src/http),
with `google-protobuf 4.33.5`. Avoid upgrading clients to the 0.10 protocol.

Use Python 3.12, Java 21+, Maven, Go 1.25+,
and Docker Compose v2. Activate the Python environment before Make commands:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r services/fraud-engine-python/requirements.txt
make contracts-build
make contracts-test
make iggy-up
make demo-java demo-python demo-ruby
make iggy-smoke
```

`make contracts-build` builds the Java jar, Go probe, and pinned Ruby image.
Ruby publishes over the Compose network; Java/Python use localhost TCP by
default. For changed credentials export `IGGY_ROOT_USERNAME` and
`IGGY_ROOT_PASSWORD`, plus Python's `IGGY_CONNECTION_STRING` with URL-encoded
credentials. Local development defaults match Compose.

For regeneration, install `protoc 33.5` (Java/Ruby runtime 4.33.5, Python
6.33.5) and `protoc-gen-go v1.36.10` (Go runtime 1.36.11). Then:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
# Add $(go env GOPATH)/bin to PATH.
# If needed, set PROTOC to the compiler path and PROTOC_INCLUDE to its include dir.
make contracts-generate
git diff --exit-code -- gen contracts/notification.desc
```

Iggy initialization waits for authenticated readiness and creates the stream,
six-partition topic and `ingest-bridge` group only when absent. Existing topic
partition/durability drift fails the job instead of silently changing routing.
The topic explicitly persists messages and consumer offsets. Docker Desktop
uses unpinned cores to avoid unsupported NUMA binding. `iggydata` is retained;
do not use `docker compose down -v` when testing resume.

The Go probe polls with `autoCommit=false` and explicitly stores the **last
processed offset**, not the next offset. It derives its next cursor from the
actual returned record offset; offsets can have gaps after server recovery.
Its dedicated smoke groups never move `ingest-bridge` offsets. This probe's
successful handling is validation plus a binary roundtrip; the production
bridge must commit only after durable Kafka acknowledgement. A dedup insert
before a Kafka write by itself is insufficient: a crash between them loses
an event. The next bridge step needs a durable outbox/recovery protocol, and
Lane 2 must tolerate the publish-ack/offset-commit duplicate window.

## Verification

`tools/test-contracts.py` runs seven fixtures through each language, compares
canonical JSON and decoded binary messages, and exercises boundary rejection.
`tools/test-sequencing.py` simulates interruption at the publish/ack boundary
for each owner and verifies exact pending retries and sequence continuation.
`harness/iggy-smoke.py` publishes Java/Python TCP and Ruby HTTP events, polls via
Go TCP without committing, kills/restarts Iggy, checks identical redelivery,
commits, kills/restarts again, checks no redelivery, then restarts producers
and checks increasing sequence and unchanged partitions. It retains all data.
