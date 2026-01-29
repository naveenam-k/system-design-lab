# Cassandra Schema

## Tables

### `user_listen_history` (raw events)
- **Purpose**: Store raw listen events from crawl workers
- **Partition Key**: `(user_id, day)` — all events for one user on one day
- **Clustering Key**: `(listened_at, event_id)` — sorted by time
- **TTL**: 7 days (automatic cleanup)

### `user_daily_topk` (counter table)
- **Purpose**: Store daily aggregated listen counts per song
- **Partition Key**: `(user_id, day)`
- **Clustering Key**: `song_id`
- **Type**: Counter table (atomic increments)
- **TTL**: None (counter tables don't support TTL, cleanup via scheduled job)

## Usage

### Initialize schema (after Cassandra is running)
```bash
cd systems/top-k-user-aggregation/implementation
chmod +x schemas/cassandra/init-schema.sh
./schemas/cassandra/init-schema.sh
```

### Manual access
```bash
docker compose exec cassandra cqlsh
```

```sql
USE topk;
DESCRIBE TABLES;

-- Check raw events
SELECT * FROM user_listen_history WHERE user_id = 'user-123' AND day = '2026-01-28' LIMIT 10;

-- Check aggregates
SELECT * FROM user_daily_topk WHERE user_id = 'user-123' AND day = '2026-01-28';

-- Increment counter (what aggregator does)
UPDATE user_daily_topk
SET listen_count = listen_count + 1
WHERE user_id = 'user-123' AND day = '2026-01-28' AND song_id = 'song-1';
```

## Counter table notes

- Counter columns are atomic — safe for concurrent increments
- Cannot mix counter and non-counter columns in same table
- Cannot use TTL on counter tables
- Need a cleanup job to delete old days (> 8 days)
