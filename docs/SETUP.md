# Local setup

## Prerequisites
- Docker Desktop (or Docker Engine) with Compose v2
- ~6 GB free RAM for the full stack
- Apache Iggy needs a Linux kernel **5.19+**. Docker Desktop on Mac/Windows already runs a recent kernel, so this is only a concern on old Linux hosts.

## Start the platform
```bash
cp .env.example .env        # optional: change passwords/versions
make up                     # or: docker compose up -d
make ps                     # everything "running"/"healthy"; kafka-init "exited (0)"
make topics                 # should list the three notifications.* topics
```

Optional tracing UI:
```bash
make up-obs                 # adds Jaeger at http://localhost:16686
```

## What's running

| Component | Purpose | From your laptop |
|---|---|---|
| Postgres 16 | dedup keys, delivery ledger, DLQ, audit | `localhost:5432` (relayguard / relayguard) |
| Apache Iggy | producer ingest streams | TCP `localhost:8090`, HTTP `localhost:3000` |
| Kafka (KRaft) | event backbone | `localhost:9094` |
| Temporal dev server | durable per-account workflows | gRPC `localhost:7233`, UI http://localhost:8233 |
| Toxiproxy | chaos between worker and adapters | API `localhost:8474` |
| Jaeger (`obs` profile) | end-to-end traces | http://localhost:16686 |

Inside the compose network, services use the names instead: `postgres:5432`, `iggy:8090`, `kafka:9092`, `temporal:7233`, `toxiproxy:2100x`.

## Smoke checks
```bash
make psql                   # \dt  -> ingested_events, deliveries, ...
make kafka-tail             # watch notifications.requested
open http://localhost:8233  # Temporal UI, namespace "relayguard"
```

## Reset everything
```bash
make reset                  # docker compose down -v (wipes all data)
```

## Adding your service to compose
Add a block like this to `docker-compose.yml` and open a PR:
```yaml
  delivery-worker:
    build: ./services/delivery-worker-go
    depends_on:
      kafka-init: { condition: service_completed_successfully }
      postgres:   { condition: service_healthy }
      temporal:   { condition: service_started }
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      TEMPORAL_ADDRESS: temporal:7233
      TEMPORAL_NAMESPACE: relayguard
      DATABASE_URL: postgres://relayguard:relayguard@postgres:5432/relayguard
```

## Common problems
- **Kafka clients on your laptop hang:** use `localhost:9094`, not `9092` (9092 is the in-network listener).
- **Iggy exits immediately:** check `docker compose logs iggy`; usually an old kernel or a seccomp restriction.
- **Port already in use:** something else is on 3000/5432/8233. Stop it, or change the left side of the port mapping.
- **Schema changes not applied:** init SQL only runs on an empty volume, so run `make reset`.

## AWS (Dec 7 demo)
Same containers, with Postgres moved to **RDS**. Plan to cover at the Nov 23 milestone.
