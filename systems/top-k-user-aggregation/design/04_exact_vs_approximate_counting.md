# Exact vs Approximate Counting in Distributed Systems

A deep dive into trade-offs when building aggregation systems that handle duplicates.

---

## The Core Problem

```
Event arrives → Should I count it?

Challenges:
  1. Same event can arrive multiple times (retries, replays, DLQ)
  2. Multiple consumers might see the same event (rebalancing)
  3. Crashes can cause re-processing
  
Result without dedup:
  OVER-COUNTING (same event counted multiple times)
```

---

## The Trade-off Triangle

```
                        EXACTNESS
                           ▲
                          /|\
                         / | \
                        /  |  \
                       /   |   \
                      /    |    \
                     /     |     \
                    /      |      \
                   /       |       \
                  ▼        ▼        ▼
            STORAGE    READ SPEED   WRITE SPEED
```

**You can optimize for 2, but not all 3:**

| Approach | Exactness | Storage | Read Speed | Write Speed |
|----------|-----------|---------|------------|-------------|
| Raw Events + Query | ✅ Exact | ❌ High | ❌ Slow | ✅ Fast |
| Raw Events + Batch Pre-compute | ✅ Exact | ❌ High | ✅ Fast | ✅ Fast |
| Counter + Bloom Filter | ❌ Approximate | ✅ Low | ✅ Fast | ✅ Fast |
| Counter + Idempotent Check | ⚠️ Near-exact | ⚠️ Medium | ✅ Fast | ⚠️ Medium |

---

## Approach 1: Exact Count (Raw Events + Batch)

### How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│  WRITE PATH: Store every event with unique ID                       │
│                                                                     │
│  Event (event_id=abc123) → INSERT INTO events (event_id PK)        │
│                            Duplicate INSERT = overwrite (no-op)     │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BATCH PATH: Pre-compute aggregates (hourly/daily)                  │
│                                                                     │
│  SELECT user_id, item_id, COUNT(DISTINCT event_id) as count        │
│  FROM events                                                        │
│  GROUP BY user_id, item_id                                         │
│  → Write to aggregates table                                        │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  READ PATH: Query pre-computed aggregates                           │
│                                                                     │
│  SELECT * FROM aggregates WHERE user_id = ? ORDER BY count LIMIT K │
└─────────────────────────────────────────────────────────────────────┘
```

### Schema

```sql
-- Raw events (source of truth)
CREATE TABLE events (
    user_id     TEXT,
    day         DATE,
    event_id    TEXT,       -- Unique ID per event
    item_id     TEXT,
    timestamp   TIMESTAMP,
    PRIMARY KEY ((user_id, day), event_id)  -- event_id ensures uniqueness
);

-- Pre-computed aggregates
CREATE TABLE daily_aggregates (
    user_id     TEXT,
    day         DATE,
    item_id     TEXT,
    count       BIGINT,     -- Exact count from batch job
    PRIMARY KEY ((user_id, day), item_id)
);
```

### Why It's Exact

```
Duplicate event arrives:
  INSERT INTO events (event_id='abc123', ...) 
  → Same primary key
  → Overwrites existing row
  → Still only 1 row for event_id='abc123'

Batch aggregation:
  COUNT(*) from events table
  → Each event_id counted exactly once
  → Result is EXACT
```

### Trade-offs

| Pros | Cons |
|------|------|
| ✅ 100% accurate | ❌ High storage (raw events) |
| ✅ Can re-compute anytime | ❌ Batch latency (not real-time) |
| ✅ Audit trail (raw data) | ❌ Complex batch infrastructure |
| ✅ Simple dedup (DB handles it) | ❌ Query cost at scale |

### When to Use

- **Ad-click billing** (legal/financial requirements)
- **Financial transactions** (audit requirements)
- **Analytics dashboards** (batch refresh OK)
- **Any system where exactness > real-time**

---

## Approach 2: Approximate Count (Bloom Filter + Counters)

### How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│  WRITE PATH: Check Bloom Filter, then increment counter             │
│                                                                     │
│  Event (event_id=abc123)                                           │
│       │                                                             │
│       ▼                                                             │
│  BF.ADD bloom_filter "abc123"                                      │
│       │                                                             │
│       ├── Returns 1 (new) → Increment counter                      │
│       └── Returns 0 (exists) → Skip (dedup)                        │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STORAGE: Counter table (pre-aggregated)                            │
│                                                                     │
│  UPDATE counters SET count = count + N                              │
│  WHERE user_id = ? AND item_id = ?                                 │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│  READ PATH: Direct lookup (O(1))                                    │
│                                                                     │
│  SELECT * FROM counters WHERE user_id = ? ORDER BY count LIMIT K   │
└─────────────────────────────────────────────────────────────────────┘
```

### Schema

```sql
-- Counter table (pre-aggregated)
CREATE TABLE counters (
    user_id     TEXT,
    day         DATE,
    item_id     TEXT,
    count       COUNTER,    -- Atomic increment
    PRIMARY KEY ((user_id, day), item_id)
);
```

### Why It's Approximate

```
Sources of inaccuracy:

1. Bloom Filter false positive (~0.1%):
   - New event looks like duplicate
   - Skipped → Under-count

2. Crash before counter update:
   - Bloom filter has event
   - Counter not incremented
   - Replay skipped → Under-count

3. Bloom Filter doesn't prevent all duplicates:
   - Different day's filter won't catch old duplicate
   - DLQ replay after TTL expires → Over-count (rare)

Overall: ~0.1-1% error margin (mostly under-count)
```

### Trade-offs

| Pros | Cons |
|------|------|
| ✅ Low storage (counters only) | ❌ ~0.1-1% error |
| ✅ Real-time reads | ❌ Can't re-compute from counters |
| ✅ Fast writes | ❌ No audit trail |
| ✅ Simple infrastructure | ❌ Complex dedup logic |

### When to Use

- **Top-K rankings** (small errors don't change rankings)
- **Real-time dashboards** (approximate OK)
- **High-volume metrics** (exactness not critical)
- **Any system where speed > exactness**

---

## Approach 3: Hybrid (Best of Both)

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     REAL-TIME PATH (Approximate)                    │
│                                                                     │
│  Event → Bloom Filter → Counter Table → Real-time Dashboard        │
│          (fast, ~99% accurate)                                      │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Also write to:
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     RAW EVENT STORAGE                               │
│                                                                     │
│  Event → Raw Events Table (event_id PK)                            │
│          (source of truth for batch)                                │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Daily batch:
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     BATCH PATH (Exact)                              │
│                                                                     │
│  Raw Events → COUNT(*) → Exact Aggregates → Billing/Reports        │
│              (slow, 100% accurate)                                  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     RECONCILIATION                                  │
│                                                                     │
│  Compare: Real-time counts vs Batch counts                         │
│  If diff > threshold → Alert + investigate                         │
└─────────────────────────────────────────────────────────────────────┘
```

### When to Use

- **Ad-tech systems** (real-time for dashboards, batch for billing)
- **E-commerce** (real-time for UX, batch for analytics)
- **Any system needing both real-time AND exact numbers**

---

## Deduplication Strategies Compared

| Strategy | Exactness | Latency | Storage | Complexity |
|----------|-----------|---------|---------|------------|
| **DB Primary Key** | ✅ Exact | Medium | High | Low |
| **Bloom Filter** | ⚠️ ~99.9% | Low | Low | Medium |
| **Redis SET** | ✅ Exact | Low | High (RAM) | Low |
| **Kafka Exactly-Once** | ✅ Exact* | Low | Low | High |
| **Flink Checkpointing** | ✅ Exact | Low | Medium | High |

*Kafka exactly-once only works within Kafka ecosystem, not for external sinks.

---

## Decision Framework

```
START
  │
  ▼
Is exact count legally/financially required?
  │
  ├── YES → Use EXACT approach (raw events + batch)
  │
  └── NO
        │
        ▼
      Is real-time critical?
        │
        ├── NO → Use EXACT approach (simpler, more accurate)
        │
        └── YES
              │
              ▼
            Is ~1% error acceptable?
              │
              ├── YES → Use APPROXIMATE approach (Bloom + counters)
              │
              └── NO → Use HYBRID approach (real-time + batch reconciliation)
```

---

## Interview Talking Points

### Q: "How would you count ad clicks without over-counting?"

```
"I'd use an idempotent storage approach:

1. Every click has a unique click_id (hash of user + ad + timestamp)
2. Store raw clicks with click_id as primary key
3. Duplicate INSERT = overwrite same row = no double-count
4. Pre-compute aggregates in daily batch jobs
5. API reads from pre-computed table

This gives exact counts because dedup is enforced by the database,
and we can always re-aggregate from raw data if needed.

Trade-off: Higher storage cost, but guaranteed correctness for billing."
```

### Q: "What if you need real-time AND exact counts?"

```
"I'd use a hybrid approach:

1. Real-time path: Bloom filter + counters for dashboards (~99% accurate)
2. Batch path: Raw events → daily aggregation for billing (100% accurate)
3. Reconciliation: Compare real-time vs batch, alert on discrepancies

This gives fast real-time views for UX while ensuring billing is exact."
```

### Q: "Why use Bloom filter instead of just storing event IDs?"

```
"Bloom filters are space-efficient:
- 10M event IDs in Redis SET = ~500MB RAM
- 10M event IDs in Bloom filter = ~1.5MB

Trade-off: Bloom has ~0.1% false positive rate (skips some new events),
but for use cases where small under-count is OK (like Top-K rankings),
the memory savings are worth it."
```

---

## Our Top-K System Design Choice

```
Requirement:
  - Over-counting: NOT acceptable
  - Under-counting: Few misses OK
  - Staleness: 1 day OK

Choice: APPROXIMATE (Bloom Filter + Counters + At-Most-Once)

Why:
  1. Under-count is acceptable → Bloom filter OK
  2. Real-time not critical → Batch would work, but counters simpler
  3. 1B events/day → Raw event storage expensive (~700GB/week)
  4. Top-K rankings: ~1% error doesn't change results significantly

Order: BF.ADD → Cassandra → Commit Offset
  - BF.ADD first: catches duplicates from source/replay
  - Cassandra before commit: data persisted before offset moves
  - Commit last: safe to replay (bloom catches duplicates)
```

---

## Summary Table

| Use Case | Recommended Approach | Why |
|----------|---------------------|-----|
| **Ad-click billing** | Exact (raw + batch) | Legal/financial requirements |
| **Real-time Top-K** | Approximate (bloom + counters) | Speed, small error OK |
| **E-commerce analytics** | Hybrid | Real-time UX + exact reports |
| **Financial transactions** | Exact (raw + 2PC) | Audit requirements |
| **Social media metrics** | Approximate | Scale, exactness not critical |

---

*Document created: 2026-01-30*
*System: Top-K User Aggregation (system-design-lab)*
