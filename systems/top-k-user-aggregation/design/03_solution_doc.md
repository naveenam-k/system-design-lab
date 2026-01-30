Document 3
Solution / Implementation Document – Code Structure & APIs

(Concrete, actionable, build-ready)

Top-K Music Aggregation Platform

Author: Naveenam
Project: system-design-lab
Scope: Service Boundaries, APIs, Schemas, Build Plan

1. Purpose of This Document

This document answers:

How exactly do we implement the system described in the design document using the chosen technologies?

It defines:

service boundaries

repo structure

APIs and events

schemas and tables

execution flow

build order

After this document, coding can begin immediately.

2. Repository Structure

Single mono-repo for simplicity:

```
system-design-lab/
├── cmd/
│   ├── oauth-service/
│   ├── crawl-scheduler/
│   ├── crawl-worker/
│   ├── raw-event-processor/
│   ├── aggregator/
│   └── api-server/
├── internal/
│   ├── kafka/
│   ├── storage/
│   ├── models/
│   ├── config/
│   ├── metrics/
│   └── utils/
├── deploy/
│   └── docker-compose.yml
├── schemas/
│   ├── kafka/
│   └── db/
└── README.md
```

3. Services & Responsibilities
3.1 OAuth Service

Binary: oauth-service

Responsibilities

Handle OAuth callbacks

Store / refresh tokens

APIs

POST /oauth/callback
GET  /oauth/tokens/{user_id}


Storage

PostgreSQL

3.2 Crawl Scheduler

Binary: crawl-scheduler

Responsibilities

Identify crawl-eligible users

Atomically enqueue crawl jobs

Execution

Periodic job (cron-like loop)

Logic

```sql
UPDATE user_crawl_schedule
SET status = 'ENQUEUED'
WHERE next_crawl_at <= now()
  AND status = 'IDLE'
RETURNING *;
```

Output

Publishes crawl jobs to Kafka topic crawl.jobs

3.3 Crawl Worker

Binary: crawl-worker

Responsibilities

Consume crawl jobs

Fetch listen history from provider API

Normalize events

Publish listen events to Kafka

Input Topic

crawl.jobs


Output Topic

user.listen.raw


Important

Stateless

Rate-limited

Updates crawl schedule after success

3.4 Event Log (Kafka)

Topics

| Topic | Purpose |
|-------|---------|
| crawl.jobs | Crawl work queue |
| user.listen.raw | Normalized listen events |

3.5 Raw Event Processor

Binary: raw-event-processor

Responsibilities

Consume raw listen events

Best-effort deduplication

Persist raw events

Input Topic

user.listen.raw


Storage

Cassandra

Table

UserListenHistory
PK: (user_id, day)
CK: listened_at
TTL: 7–30 days

3.6 Aggregation Service

Binary: aggregator

Responsibilities

Consume raw listen events

Aggregate counts in memory

Periodically flush to storage

Input Topic

user.listen.raw


Aggregation Key

(user_id, day, song_id)


Flush Strategy

Every 60 seconds OR

Batch size threshold

Offset Commit

After successful flush

3.7 Aggregated Store

Table

UserDailyTopK


Schema

PK: (user_id, day)
CK: song_id
listen_count int
TTL: 8 days

3.8 Read API (Top-K Service)

Binary: api-server

Responsibilities

Serve Top-K requests

Merge daily aggregates

Cache results

API

GET /users/{user_id}/topk?days=7&k=10


Flow

Fetch last N days from UserDailyTopK

Merge in memory

Sort Top-K

Cache response (Redis)

4. Data Models (Code-Level)

**ListenEvent (Kafka)**
```json
{
  "event_id": "uuid",
  "user_id": "string",
  "song_id": "string",
  "provider": "spotify",
  "listened_at": "timestamp"
}
```

**CrawlJob (Kafka)**
```json
{
  "user_id": "string",
  "provider": "spotify",
  "since": "timestamp"
}
```

5. Configuration & Environment

All services configured via env vars

Shared config module

Example:

KAFKA_BROKERS
POSTGRES_DSN
CASSANDRA_HOSTS
REDIS_ADDR

6. Local Deployment

Docker Compose includes

Kafka + Zookeeper

Cassandra

PostgreSQL

Redis

All services

Startup order:

Infra (Kafka, DBs)

OAuth + Scheduler

Workers

Aggregator

API server

7. Build Order (Critical)

This is the recommended implementation sequence:

Kafka + topics

Crawl scheduler → crawl worker → Kafka

Raw event processor → Cassandra

Aggregator → UserDailyTopK

Top-K API

Cache & metrics

8. Failure Handling (Implementation-Level)

Worker crash → job retried

Aggregator crash → Kafka replay

Partial flush → possible over-count

Cache failure → fallback to DB

9. Testing Strategy

Unit tests per service

Integration tests with Docker Compose

Failure injection:

kill aggregator mid-flush

restart Kafka consumer

Replay tests from Kafka offsets

10. What This Document Enables

After this document:

Repo structure is clear

APIs are defined

Schemas are known

Build can proceed without ambiguity

11. Final Mental Model

Design doc → what & why

Technology doc → with what

Solution doc → how exactly

Code → proof

✅ You are now at the “start coding” point