-- Cassandra schema for Top-K aggregation system
-- Run with: cqlsh -f init.cql

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS topk
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE topk;

-- Raw listen events (regular table with TTL)
-- Partition: (user_id, day) — all events for one user on one day
-- Clustering: listened_at, event_id — sorted by time, unique by event_id
CREATE TABLE IF NOT EXISTS user_listen_history (
    user_id     TEXT,
    day         DATE,
    listened_at TIMESTAMP,
    event_id    TEXT,
    song_id     TEXT,
    provider    TEXT,
    PRIMARY KEY ((user_id, day), listened_at, event_id)
) WITH default_time_to_live = 604800  -- 7 days TTL
  AND CLUSTERING ORDER BY (listened_at DESC);

-- Daily aggregates (counter table)
-- Partition: (user_id, day) — all song counts for one user on one day
-- Clustering: song_id — each song is a row
-- Note: Counter tables cannot have TTL, cleanup via scheduled job
CREATE TABLE IF NOT EXISTS user_daily_topk (
    user_id      TEXT,
    day          DATE,
    song_id      TEXT,
    listen_count COUNTER,
    PRIMARY KEY ((user_id, day), song_id)
);

-- Note: Secondary indexes are NOT allowed on counter tables.
-- For cleanup, query by (user_id, day) directly or use a separate tracking table.
