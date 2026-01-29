# Raw Event Processor

Consumes listen events from Kafka and writes them to Cassandra for durable storage.

## Flow

```
Kafka (user.listen.raw)
         │
         ▼
┌─────────────────────┐
│ Raw Event Processor │
│                     │
│ - Consume event     │
│ - Write to Cassandra│
│ - Commit offset     │
└─────────┬───────────┘
          │
          ▼
Cassandra (user_listen_history)
```

## Why this service exists

- **Durability**: Raw events are stored for replay, debugging, recomputation
- **Separation of concerns**: Aggregator handles counting, this handles storage
- **Independent scaling**: Can scale separately from aggregator

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
| CASSANDRA_HOSTS | cassandra:9042 | Cassandra host(s) |
| CONSUMER_GROUP | raw-event-processor | Kafka consumer group ID |

## Verify data in Cassandra

```bash
docker compose exec cassandra cqlsh -e "
  USE topk;
  SELECT * FROM user_listen_history LIMIT 10;
"
```
