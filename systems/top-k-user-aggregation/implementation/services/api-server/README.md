# Top-K API Server

Serves the Top-K songs API for users.

## API

### `GET /users/{user_id}/topk`

Returns the top K most-listened songs for a user over the last N days.

**Query Parameters:**
| Param | Default | Description |
|-------|---------|-------------|
| `days` | 7 | Number of days to aggregate (1-30) |
| `k` | 10 | Number of top songs to return (1-100) |

**Example:**
```bash
curl "http://localhost:8080/users/user-123/topk?days=7&k=10"
```

**Response:**
```json
{
  "user_id": "user-123",
  "days": 7,
  "k": 10,
  "results": [
    {"song_id": "song-42", "listen_count": 150, "rank": 1},
    {"song_id": "song-7", "listen_count": 98, "rank": 2},
    ...
  ],
  "cached": false
}
```

**Headers:**
- `X-Cache: HIT` — response from Redis cache
- `X-Cache: MISS` — computed from Cassandra

### `GET /healthz`

Health check endpoint.

## Flow

```
Client
   │
   ▼
┌─────────────────────┐
│    API Server       │
│                     │
│ 1. Check Redis cache│
│    ↓ (miss)         │
│ 2. Query Cassandra  │
│    (7 days)         │
│ 3. Merge & sort     │
│ 4. Cache in Redis   │
│ 5. Return response  │
└─────────────────────┘
   │           │
   ▼           ▼
 Redis      Cassandra
 (cache)    (user_daily_topk)
```

## Run with Docker

Part of the main `docker-compose.yml`:

```bash
cd systems/top-k-user-aggregation/implementation
docker compose up --build
```

Access at: `http://localhost:8080/users/{user_id}/topk`

## Environment variables

| Var | Default | Description |
|-----|---------|-------------|
| CASSANDRA_HOSTS | cassandra | Cassandra host(s) |
| REDIS_ADDR | redis:6379 | Redis address |
| PORT | 8080 | HTTP server port |
| CACHE_TTL | 1h | Cache TTL for Top-K results |

## Caching strategy

- Cache key: `topk:{user_id}:{days}:{k}`
- TTL: 1 hour (configurable)
- Cache is invalidated by TTL expiry (not on new events)
- This matches the "1 day staleness acceptable" requirement
