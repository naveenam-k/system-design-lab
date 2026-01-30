# Lessons Learned – Top-K User Aggregation

## What We Built (Lab Scope)

A functional end-to-end system that:
- Simulates crawling user listening history
- Publishes events to Kafka
- Persists raw events to Cassandra
- Aggregates daily counts using counter columns
- Serves Top-K via API with Redis caching

**Stack:** Go, Kafka, Cassandra, Redis, Asynq, Docker Compose

---

## Known Limitations (Lab Trade-offs)

| Area | Lab Implementation | Why It's OK for Lab |
|------|-------------------|---------------------|
| Crawl jobs | **DB-backed scheduler with reconciliation** | ✅ Production-ready pattern |
| Deduplication | **Redis Bloom Filter + At-Most-Once** | ✅ Shared, multi-aggregator safe |
| Partition affinity | ✅ Enforced via Kafka Key | Shared Bloom handles rebalancing edge cases |
| Cache invalidation | TTL-only (5 min in lab) | Data changes daily, could use 1-6 hour TTL |
| OAuth tokens | No refresh rotation | GitHub tokens are long-lived for demo |
| Error handling | Log and continue | See "Error Handling: Lab vs Production" section below |
| Metrics/Observability | None | Visual verification via logs |

---

## DB-Backed Scheduler Pattern (Implemented)

### The Problem: Redis Job Persistence

Originally, crawl jobs were scheduled using Asynq with `ProcessIn(24*time.Hour)`. The problem:
- Jobs stored in Redis (in-memory)
- Redis restart = all scheduled jobs lost
- 70M daily crawl jobs × 500 bytes = 35GB RAM (expensive at scale)

### The Solution: DB as Source of Truth

```
┌─────────────────────────────────────────────────────────────────┐
│   PostgreSQL: user_crawl_schedule                               │
│   ┌──────────┬──────────┬────────────────┬──────────────────┐  │
│   │ user_id  │ provider │ next_crawl_at  │ status           │  │
│   ├──────────┼──────────┼────────────────┼──────────────────┤  │
│   │ user_1   │ spotify  │ tomorrow 2AM   │ IDLE             │  │
│   │ user_2   │ spotify  │ now            │ ENQUEUED         │  │
│   └──────────┴──────────┴────────────────┴──────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
         Scheduler polls every 10 seconds
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│   Redis/Asynq: Ready Queue (only "execute NOW" jobs)            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│   Crawl Workers → Update DB: status=IDLE, next_crawl_at=tomorrow│
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

**1. Scheduler (crawl-scheduler)**
```sql
-- Query 1: Find ready jobs
SELECT * FROM user_crawl_schedule
WHERE next_crawl_at <= NOW() AND status = 'IDLE'
FOR UPDATE SKIP LOCKED;
→ Set status = 'ENQUEUED', enqueue to Asynq

-- Query 2: Reconciliation (find stuck jobs)
SELECT * FROM user_crawl_schedule
WHERE status = 'ENQUEUED' AND updated_at < NOW() - INTERVAL '1 hour';
→ Re-enqueue (job was lost in Redis)
```

**2. Worker (crawl-worker)**
```go
// On job start
updateStatus(userID, provider, "RUNNING")

// On success
UPDATE user_crawl_schedule
SET status = 'IDLE', next_crawl_at = NOW() + '24 hours'
WHERE user_id = ? AND provider = ?
```

### Why This Works

| Scenario | What Happens |
|----------|--------------|
| Normal flow | IDLE → ENQUEUED → RUNNING → IDLE (next_crawl_at = tomorrow) |
| Redis dies | DB has status='ENQUEUED', reconciliation re-enqueues |
| Worker crashes | Same as above |
| Scheduler restarts | Continues polling from DB |

### Trade-offs

| Aspect | DB-Backed | Redis-Only |
|--------|-----------|------------|
| Durability | ✅ Survives restart | ❌ Lost on restart |
| Cost at scale | ✅ Disk storage | ❌ RAM (expensive) |
| Latency | ~10s poll delay | Instant |
| Complexity | Medium | Low |
| Reconciliation | ✅ Built-in | ❌ Manual |

---

## Partition Affinity + Shared Bloom Filter (Defense in Depth)

### How Partition Affinity Works

```go
// In crawl-worker/tasks/crawl.go
msgs = append(msgs, kafka.Message{
    Key:   []byte(e.UserID),  // ← Kafka routes by this key
    Value: data,
})
```

Kafka automatically hashes the key: `hash(UserID) % num_partitions → consistent partition`

Result: **All events for the same user always go to the same partition → same aggregator.**

### Mental Model: Why Both?

```
Normal case (99.9%):
  Partition affinity via Key → Same aggregator → Local dedup would suffice
  But Bloom Filter is cheap, so why not keep it?

Edge cases (0.1%):
  - Consumer rebalancing (scale up/down aggregators)
  - Aggregator crash + restart mid-batch
  - Kafka partition reassignment
  
  → Different aggregator might see replayed events
  → Shared Redis Bloom Filter catches these

Defense in Depth:
  Partition affinity = efficient common path
  Shared Bloom Filter = safety net for edge cases
  Both together = robust deduplication
```

### Trade-off: Shared vs Local Bloom Filter

| Aspect | Shared Redis Bloom | Local In-Memory Bloom |
|--------|-------------------|----------------------|
| Rebalancing safe | ✅ Yes | ❌ No (new aggregator has empty filter) |
| Latency | ~1ms Redis call | ~0ms (memory) |
| Memory | Redis server | Each aggregator instance |
| Complexity | Medium | Low |

**Our choice:** Shared Redis Bloom — handles all edge cases, ~1ms overhead acceptable.

---

## Error Handling: Lab vs Production Mental Model

### Lab Approach: Log and Continue

```go
// Lab code pattern
if err != nil {
    log.Printf("Error: %v", err)
    // continue processing...
}
```

**Why it's OK for lab:** Visual debugging via logs, no SLA, manual intervention acceptable.

### Production Mental Model: Classify → Handle → Alert → Recover

```
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 1: CLASSIFY THE ERROR                                             │
│                                                                         │
│  ┌─────────────────┬────────────────┬─────────────────────────────────┐│
│  │ Type            │ Retryable?     │ Examples                        ││
│  ├─────────────────┼────────────────┼─────────────────────────────────┤│
│  │ Transient       │ ✅ Yes         │ Network timeout, DB busy        ││
│  │ Permanent       │ ❌ No          │ Invalid data, auth failure      ││
│  │ Dependency down │ ✅ Yes (later) │ Kafka unavailable, Redis down   ││
│  │ Bug             │ ❌ No          │ Nil pointer, logic error        ││
│  └─────────────────┴────────────────┴─────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 2: HANDLE APPROPRIATELY                                           │
│                                                                         │
│  Transient → Retry with exponential backoff (1s, 2s, 4s, 8s, max 30s)  │
│  Permanent → Send to Dead Letter Queue (DLQ) + alert                   │
│  Dependency down → Circuit breaker (fail fast) + health check          │
│  Bug → Crash (let orchestrator restart) + alert immediately            │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 3: ALERT THE RIGHT PEOPLE                                         │
│                                                                         │
│  ┌──────────────────┬─────────────────┬───────────────────────────────┐│
│  │ Severity         │ Response Time   │ Channel                       ││
│  ├──────────────────┼─────────────────┼───────────────────────────────┤│
│  │ Critical (P1)    │ < 5 min         │ PagerDuty → Phone call        ││
│  │ High (P2)        │ < 30 min        │ Slack alert → On-call         ││
│  │ Medium (P3)      │ < 4 hours       │ Ticket created                ││
│  │ Low (P4)         │ Next sprint     │ Log aggregation only          ││
│  └──────────────────┴─────────────────┴───────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  STEP 4: RECOVER (Graceful Degradation)                                 │
│                                                                         │
│  API Server: Redis down → Serve from Cassandra (slower, but works)     │
│  Aggregator: Cassandra down → Buffer in memory (risk data loss)        │
│  Crawl Worker: Kafka down → Return error, scheduler will retry         │
│  All Services: Dependency flapping → Circuit breaker (stop trying)     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Production Patterns to Implement

**1. Structured Error Context**
```go
// Bad (lab)
log.Printf("Error writing to Cassandra: %v", err)

// Good (production)
log.Error("cassandra_write_failed",
    "user_id", userID,
    "song_id", songID,
    "error", err.Error(),
    "retry_count", retryCount,
    "latency_ms", latency,
)
```

**2. Retry with Backoff**
```go
// Exponential backoff for transient errors
for attempt := 0; attempt < maxRetries; attempt++ {
    err := doOperation()
    if err == nil {
        return nil
    }
    if !isRetryable(err) {
        return err // Don't retry permanent errors
    }
    sleep := time.Duration(math.Pow(2, float64(attempt))) * time.Second
    time.Sleep(min(sleep, maxBackoff))
}
return sendToDLQ(item) // All retries exhausted
```

**3. Circuit Breaker**
```go
// Fail fast when dependency is unhealthy
if circuitBreaker.IsOpen() {
    return ErrServiceUnavailable // Don't even try
}
err := callDependency()
if err != nil {
    circuitBreaker.RecordFailure()
} else {
    circuitBreaker.RecordSuccess()
}
```

**4. Dead Letter Queue (DLQ)**
```
Normal flow: Event → Process → Success
Error flow:  Event → Process → Fail → Retry 3x → DLQ

DLQ allows:
  - Manual inspection of failed events
  - Replay after bug fix
  - Metrics on failure patterns
```

**5. Health Endpoints**
```
GET /healthz → Am I running? (liveness probe)
GET /readyz  → Can I serve traffic? (readiness probe)

Kubernetes uses these to:
  - Restart unhealthy pods (liveness fails)
  - Stop sending traffic (readiness fails)
```

### Alert Thresholds (What to Monitor)

| Metric | Warning | Critical | Why |
|--------|---------|----------|-----|
| Kafka consumer lag | > 10K | > 100K | Processing falling behind |
| Error rate | > 1% | > 5% | Something is broken |
| p99 latency | > 500ms | > 2s | Performance degradation |
| DLQ size | > 100 | > 1000 | Failures accumulating |
| Cache hit rate | < 80% | < 50% | Cache not effective |
| Disk usage | > 70% | > 85% | Running out of space |

### Summary: Lab → Production Checklist

```
□ Replace log.Printf with structured logging (JSON)
□ Add error classification (transient vs permanent)
□ Implement retry with exponential backoff
□ Add circuit breakers for external dependencies
□ Set up DLQ for failed events
□ Add /healthz and /readyz endpoints
□ Configure alerting thresholds
□ Create runbook for common failures
□ Test graceful degradation paths
□ Load test error scenarios
```

---

## Production Considerations

### 1. **Reliability & Durability**

| Concern | Lab | Production |
|---------|-----|------------|
| Kafka | Single broker, no replication | 3+ brokers, replication factor 3, ISR ≥ 2 |
| Cassandra | Single node | 3+ nodes, RF=3, LOCAL_QUORUM reads/writes |
| Redis | Single instance | Redis Cluster or Sentinel for HA |
| Job persistence | **DB-backed scheduler** ✅ | Same pattern scales to production |

### 2. **Scalability**

| Component | Lab | Production |
|-----------|-----|------------|
| Crawl workers | 1 instance | Auto-scaled based on queue depth |
| Aggregators | 1 instance | Partition-affine consumers (1 per Kafka partition) |
| API servers | 1 instance | Horizontally scaled behind load balancer |
| Kafka partitions | Default | Partition by `user_id` for ordering guarantees |

### 3. **Data Integrity**

```
Requirement: Over-counting NOT acceptable, few misses OK

Implemented:
  1. Redis Bloom Filter deduplication (shared across aggregators)
     - Key per day: dedup:2026-01-30
     - Capacity: 10M events/day, 0.1% false positive rate
     - TTL: 8 days (auto-rotation)
     - False positive → skip event → under-count (OK!)
     - Multi-aggregator safe: all check the SAME bloom filter
  
  2. Order: BF.ADD → Cassandra → Commit Offset
     - Crash after BF.ADD, before Cassandra → replay, bloom skips (under-count OK)
     - Crash after Cassandra, before commit → replay, bloom skips (CORRECT! data persisted)
     - Crash after commit → no replay (CORRECT!)
     - Guarantees NO over-counting
```

### 4. **Observability (Must-Have for Production)**

| Tool | Purpose |
|------|---------|
| Prometheus + Grafana | Metrics (lag, throughput, latencies) |
| Structured logging (JSON) | Queryable logs |
| Distributed tracing (Jaeger/Zipkin) | Request flow debugging |
| Alerting (PagerDuty/Opsgenie) | On-call notifications |

**Key metrics to track:**
- Kafka consumer lag per group
- Cassandra write latency (p99)
- API response time (p50, p95, p99)
- Cache hit rate
- Crawl job failure rate

### 5. **Security**

| Concern | Lab | Production |
|---------|-----|------------|
| OAuth tokens | SQLite, plaintext | Encrypted at rest (Vault, KMS) |
| API auth | None | JWT/OAuth2 bearer tokens |
| Network | Docker bridge | VPC, private subnets, TLS everywhere |
| Secrets | `.env` files | Secret manager (AWS Secrets Manager, Vault) |

### 6. **Deployment**

| Aspect | Lab | Production |
|--------|-----|------------|
| Orchestration | Docker Compose | Kubernetes (EKS/GKE) |
| CI/CD | Manual | GitHub Actions → ArgoCD |
| Config | Environment variables | ConfigMaps + Secrets |
| Rollbacks | `docker compose down` | Kubernetes rollback, blue-green |

### 7. **Data Management**

| Concern | Lab | Production |
|---------|-----|------------|
| Backups | None | Cassandra snapshots, Kafka MirrorMaker |
| TTL enforcement | Cassandra TTL | TTL + compaction tuning |
| Schema migrations | Manual CQL | Liquibase/Flyway or versioned CQL scripts |
| Data retention | 7 days | Compliance-driven (GDPR delete on request) |

---

## What Would Change at Scale

### 10K users → 10M users

1. **Kafka partitions**: Increase to 50-100, partition by `user_id % N`
2. **Aggregator scaling**: One consumer per partition (consumer group handles this)
3. **Cassandra**: 
   - Partition key tuning to avoid hot spots
   - Consider `(user_id, day)` vs `(user_id)` based on access patterns
4. **API caching**: 
   - Multi-tier: Local cache → Redis → Cassandra
   - Cache warming for active users

### 1M events/day → 1B events/day

1. **Kafka**: 
   - Multiple clusters (geo-distributed)
   - Tiered storage for cold data
2. **Aggregation**:
   - Pre-aggregation at crawl-worker level (micro-batching)
   - Stream processing (Flink/Spark Streaming) instead of simple consumers
3. **Storage**:
   - Time-series optimized store (ClickHouse, TimescaleDB) for analytics
   - Cassandra for serving, analytics DB for exploration

#### Deep Dive: Pre-Aggregation at Crawl Worker

```
BEFORE (current - individual events):
┌─────────────────────────────────────────────────────────────────┐
│ Crawl Worker fetches 100 listen events for Alice                │
│                                                                 │
│ Publishes to Kafka:                                             │
│   Event 1: {user: alice, song: X}                              │
│   Event 2: {user: alice, song: X}                              │
│   Event 3: {user: alice, song: X}                              │
│   ... (100 individual events)                                   │
└─────────────────────────────────────────────────────────────────┘
              │
              ▼ 100 Kafka messages
              
┌─────────────────────────────────────────────────────────────────┐
│ Aggregator: Process 100 messages, accumulate, flush             │
└─────────────────────────────────────────────────────────────────┘


AFTER (pre-aggregation - summaries):
┌─────────────────────────────────────────────────────────────────┐
│ Crawl Worker fetches 100 listen events for Alice                │
│                                                                 │
│ Pre-aggregates locally:                                         │
│   {user: alice, song: X, count: 3}                             │
│   {user: alice, song: Y, count: 2}                             │
│   ... (maybe 20 unique songs)                                   │
│                                                                 │
│ Publishes to Kafka:                                             │
│   Summary 1: {user: alice, song: X, count: 3}                  │
│   Summary 2: {user: alice, song: Y, count: 2}                  │
│   ... (20 messages instead of 100)                              │
└─────────────────────────────────────────────────────────────────┘
              │
              ▼ 20 Kafka messages (80% reduction!)
              
┌─────────────────────────────────────────────────────────────────┐
│ Aggregator: Process 20 messages, much less work                 │
└─────────────────────────────────────────────────────────────────┘

Benefits:
  - 80% fewer Kafka messages
  - 80% less work for aggregator
  - Network bandwidth savings
  - Crawl worker has local data anyway, aggregation is cheap

Trade-off:
  - Slightly more complex crawl worker
  - Loss of raw event granularity in Kafka (if needed for replay/debugging)
```

#### Deep Dive: Why Stream Processing (Flink/Spark) at Scale

**Simple Kafka consumer limitations at 1B events/day (~11,500 events/sec):**

```
Simple Consumer (our current approach):
┌─────────────────────────────────────────────────────────────────┐
│  Problems at scale:                                             │
│                                                                 │
│  1. Memory: Each consumer holds state for its partition         │
│     → OOM risk with billions of unique keys                     │
│                                                                 │
│  2. Checkpointing: Manual offset commit                         │
│     → Crash between flush and commit = data loss or duplicates  │
│                                                                 │
│  3. Windowing: "Past 7 days" logic is manual                   │
│     → Error-prone, late arrival handling is DIY                 │
│                                                                 │
│  4. Backpressure: No built-in flow control                      │
│     → Cassandra slow? Consumer keeps reading → OOM              │
│                                                                 │
│  5. Scaling: Adding partitions requires rebalancing             │
│     → State redistribution is manual                            │
└─────────────────────────────────────────────────────────────────┘
```

**What Flink/Spark Streaming provide:**

```
┌─────────────────────────────────────────────────────────────────┐
│  1. DISTRIBUTED STATE MANAGEMENT                                │
│     State stored in RocksDB (disk-backed, not just memory)      │
│     Can handle TB of state across cluster                       │
│     Automatic state partitioning and redistribution             │
│                                                                 │
│  2. EXACTLY-ONCE CHECKPOINTING                                  │
│     Framework snapshots state + offsets atomically              │
│     Crash → restore from checkpoint → no data loss              │
│     No manual offset management                                 │
│                                                                 │
│  3. BUILT-IN WINDOWING                                          │
│     .window(TumblingWindow.of(Time.days(1)))                    │
│     .aggregate(new CountAggregator())                           │
│     Framework handles late arrivals, watermarks                 │
│                                                                 │
│  4. BACKPRESSURE                                                │
│     If sink (Cassandra) is slow, framework slows down source    │
│     Prevents OOM, no manual flow control needed                 │
│                                                                 │
│  5. DYNAMIC SCALING                                             │
│     Add/remove workers without stopping pipeline                │
│     State automatically redistributed                           │
└─────────────────────────────────────────────────────────────────┘
```

**When to use what:**

| Scale | Approach | Why |
|-------|----------|-----|
| < 1M events/day | Simple consumer | Low complexity, works fine |
| 1M - 100M events/day | Pre-aggregation + simple consumer | Reduces load, still manageable |
| 100M - 1B events/day | Flink/Spark Streaming | Need distributed state, checkpointing |
| > 1B events/day | Flink + pre-aggregation + tiered storage | All optimizations needed |

---

## Key Learnings from This Build

### Architecture
- **Event log as backbone works** — decouples ingestion from processing
- **Counter columns simplify concurrent writes** — no read-modify-write needed
- **Async aggregation is key** — never aggregate on read path

### Technology Choices
- **DB + Asynq for job scheduling** — DB for durability, Asynq for execution
- **Reconciliation pattern** — recovers from Redis failures automatically
- **Kafka for event log** — durable, replayable, multiple consumers
- **Kafka Key for partition affinity** — same user → same partition → same consumer
- **Shared Bloom Filter** — defense in depth for rebalancing edge cases
- **Cassandra counters have quirks** — no secondary indexes, no TTL on counters directly

### Development
- **Docker Compose is sufficient for labs** — fast iteration, easy cleanup
- **Bind mounts > named volumes for debugging** — can inspect data directly
- **Start with design doc** — prevents scope creep and rework

### What I'd Do Differently
1. ~~**Implement deduplication from day 1**~~ — ✅ Done (Bloom Filter + At-Most-Once)
2. **Add structured logging from day 1** — debugging distributed systems is hard
3. **Add health endpoints** — `/healthz` and `/readyz` for each service
4. **Schema versioning** — track CQL changes in migration files

---

## Talking Points

When discussing this system:

1. **Why pull-based ingestion?**
   - Third-party APIs don't push; we control crawl rate and backoff

2. **Why Kafka + Cassandra?**
   - Kafka: durable log, replay capability, multiple consumers
   - Cassandra: high write throughput, time-series friendly, TTL built-in

3. **How do you handle failures?**
   - DB-backed scheduler with reconciliation for lost jobs
   - Kafka consumer offsets for replay
   - Idempotent writes (event_id) to prevent over-counting on replay
   - Cache fallback to DB

4. **How would you scale this?**
   - Horizontal: more partitions, more consumers, more API servers
   - Vertical: bigger Cassandra nodes for hot users

5. **What's the consistency model?**
   - Eventual consistency with ~1 day staleness acceptable
   - Strong consistency not needed for "top songs" use case

---

## Checklist: Before Going to Production

- [x] **Deduplication implemented** (Redis Bloom Filter + At-Most-Once + Daily Rotation)
- [ ] Kafka replication configured (RF ≥ 3)
- [ ] Cassandra multi-node cluster (RF = 3)
- [ ] Redis HA (Sentinel or Cluster)
- [ ] All secrets in secret manager
- [ ] TLS enabled on all connections
- [ ] Prometheus metrics exposed
- [ ] Alerting configured (consumer lag, error rates)
- [ ] Health endpoints on all services
- [ ] CI/CD pipeline with automated tests
- [ ] Runbook for common failures
- [ ] Load tested to expected peak traffic
- [ ] GDPR/data deletion flow implemented

---

*Document created: 2026-01-30*
*System: Top-K User Aggregation (system-design-lab)*
