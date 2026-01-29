# Aggregator

Consumes listen events from Kafka, accumulates counts in memory, and periodically flushes to Cassandra using counter increments.

## Flow

```
Kafka (user.listen.raw)
         │
         ▼
┌─────────────────────┐
│     Aggregator      │
│                     │
│ In-memory:          │
│ (user,day,song) → N │
│                     │
│ Every 30s: flush    │
└─────────┬───────────┘
          │
          ▼ UPDATE ... SET listen_count = listen_count + N
Cassandra (user_daily_topk - counter table)
```

## Why in-memory batching?

- **Efficiency**: 1000 events for same song → 1 counter increment (+1000)
- **Reduced Cassandra load**: Fewer writes, higher throughput
- **Atomic counters**: Safe even with multiple aggregators (partition affinity)

## Flush strategy

- Periodic: every `FLUSH_INTERVAL` (default 30s)
- On shutdown: flush remaining counts before exit
- Kafka offset committed **after** successful flush

## Run with Docker

Part of the main `docker-compose.yml`:

```bash
cd systems/top-k-user-aggregation/implementation
docker compose up --build
```

## Environment variables

| Var | Default | Description |
|-----|---------|-------------|
| KAFKA_BROKER | kafka:9092 | Kafka broker address |
| CASSANDRA_HOSTS | cassandra | Cassandra host(s) |
| CONSUMER_GROUP | aggregator | Kafka consumer group ID |
| FLUSH_INTERVAL | 30s | How often to flush to Cassandra |

## Verify aggregates in Cassandra

```bash
docker compose exec cassandra cqlsh -e "
  USE topk;
  SELECT * FROM user_daily_topk WHERE user_id = 'user-123' AND day = '2026-01-29';
"
```

## Scaling

- Kafka partitions by `user_id` → same user always goes to same aggregator
- Safe to run multiple aggregators (they'll split partitions)
- Counter increments are atomic — no race conditions
