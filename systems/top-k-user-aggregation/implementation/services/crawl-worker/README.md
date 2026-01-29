# Crawl Worker

Asynq-based worker that:
1. Consumes scheduled crawl jobs from Redis
2. Fetches listen history from provider (simulated for now)
3. Publishes normalized events to Kafka (`user.listen.raw`)
4. Reschedules itself for tomorrow

## How it works

```
Redis (Asynq queue)
       │
       ▼
┌─────────────────┐
│  Crawl Worker   │
│                 │
│  1. Fetch data  │
│  2. Normalize   │
│  3. Publish     │
│  4. Reschedule  │
└────────┬────────┘
         │
         ▼
   Kafka (user.listen.raw)
```

## Run with Docker (recommended)

Everything runs in Docker via the parent `docker-compose.yml`:

```bash
cd systems/top-k-user-aggregation/implementation
docker compose up --build
```

The `crawl-worker` service starts automatically.

## Enqueue a test job

```bash
docker compose run --rm enqueue-test
```

This uses the `tools` profile to run a one-off container that enqueues a test crawl job.

## Run locally (alternative)

```bash
cd services/crawl-worker
go mod tidy
REDIS_ADDR=localhost:6379 KAFKA_BROKER=localhost:29092 go run .
```

## Environment variables
| Var | Default | Description |
|-----|---------|-------------|
| REDIS_ADDR | redis:6379 | Redis address for Asynq |
| KAFKA_BROKER | kafka:9092 | Kafka broker address |
