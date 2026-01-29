# Top-K Implementation (local)

Local infrastructure + services for the Top-K aggregation system.

## Architecture (simplified)

```
┌─────────────┐
│  Asynq      │  ← scheduled crawl jobs (Redis-backed)
│  Scheduler  │
└─────┬───────┘
      │
      ▼
┌─────────────┐       ┌─────────────────┐
│ Crawl       │ ────► │ Kafka           │  ← user.listen.raw
│ Worker      │       │ (event log)     │
└─────────────┘       └────────┬────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
      ┌──────────────┐  ┌──────────────┐  ┌──────────┐
      │ Raw Event    │  │ Aggregator   │  │ (future) │
      │ Processor    │  │              │  │          │
      └──────┬───────┘  └──────┬───────┘  └──────────┘
             │                 │
             ▼                 ▼
      ┌──────────────┐  ┌──────────────┐
      │ Cassandra    │  │ Cassandra    │
      │ (raw events) │  │ (aggregates) │
      └──────────────┘  └──────────────┘
```

## Infra components

| Component | Purpose |
|-----------|---------|
| Kafka + Zookeeper | Durable event log (`user.listen.raw`) |
| Postgres | Control plane (users, tokens, schedules) |
| Cassandra | Raw events + aggregates |
| Redis | Cache + Asynq job queue |

## Quick start (fully dockerized)

```bash
# 0. Create runtime directories (one-time)
chmod +x init-runtime-dirs.sh
./init-runtime-dirs.sh

# 1. Start everything (infra + services)
docker compose up --build

# 2. Create Kafka topics (one-time, in another terminal)
./create-topics.sh

# 3. Create Cassandra schema (one-time, wait ~30s for Cassandra to start)
chmod +x schemas/cassandra/init-schema.sh
./schemas/cassandra/init-schema.sh

# 4. Enqueue a test crawl job
docker compose run --rm enqueue-test
```

## Runtime data

All Docker data is stored at `/runtime/shared/system-design-lab/top_k_user_aggregation/`:
```
/runtime/shared/system-design-lab/top_k_user_aggregation/
├── zookeeper/
├── kafka/
├── postgres/
├── cassandra/
└── redis/
```

To reset all data:
```bash
sudo rm -rf /runtime/shared/system-design-lab/top_k_user_aggregation/*
./init-runtime-dirs.sh
```

## Schemas

| Store | Path | Description |
|-------|------|-------------|
| Cassandra | `schemas/cassandra/init.cql` | Keyspace, raw events table, counter table |
| Postgres | `schemas/postgres/` | (pending) Users, tokens, schedules |

## Connection points (from host)
| Service | Address |
|---------|---------|
| Kafka | `localhost:29092` |
| Postgres | `postgresql://topk:topk@localhost:5432/topk` |
| Cassandra | `localhost:9042` |
| Redis | `localhost:6379` |

## Connection points (inside Docker network)
| Service | Address |
|---------|---------|
| Kafka | `kafka:9092` |
| Postgres | `postgres:5432` |
| Cassandra | `cassandra:9042` |
| Redis | `redis:6379` |

## Services (Go)

| Service | Path | Description |
|---------|------|-------------|
| crawl-worker | `services/crawl-worker/` | Asynq worker — fetches listen history, publishes to Kafka |
| raw-event-processor | `services/raw-event-processor/` | Consumes Kafka, writes to Cassandra |
| aggregator | `services/aggregator/` | Consumes Kafka, computes daily aggregates |
| api-server | `services/api-server/` | Serves Top-K API |

## Job scheduling (Asynq)

We use [Asynq](https://github.com/hibiken/asynq) (Redis-backed) for crawl job scheduling:
- Jobs enqueued with `ProcessAt(time)` for delayed execution
- Workers self-reschedule for next day after crawl completes
- Built-in retries, dead-letter queue, dashboard
