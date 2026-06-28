# ADR-001: Choice of Apache Kafka as Event Bus

**Date**: June 2025  
**Status**: ✅ Accepted  
**Reviewers**: Architecture Team  
**Relevant to Phase**: 0 (Architecture), All subsequent phases

---

## Summary

**Decision**: Use Apache Kafka as the central event bus for distributing database changes from Debezium CDC to downstream consumers (Redis, Elasticsearch, Analytics).

**Key Rationale**:
- ✅ Exactly-once semantics (no lost events, no duplicates)
- ✅ Ordering guarantees per partition (critical for consistency)
- ✅ Horizontal scalability (add partitions/brokers as needed)
- ✅ Decoupling (Debezium publishes once; multiple consumers read independently)
- ✅ Durability (persists events to disk; survives crashes)

---

## Context: Why This Decision Matters

### The Problem We're Solving

We have:
- **One source** (PostgreSQL changes via Debezium)
- **Multiple consumers** (Redis, Elasticsearch, Analytics)

Naive approaches fail:

#### Approach 1: Direct Connections
```
Debezium --→ Redis
       \--→ Elasticsearch
        \--→ Analytics
```
**Problems**:
- ❌ If Redis is slow, it blocks Elasticsearch
- ❌ If a consumer crashes, Debezium must retry (tight coupling)
- ❌ Adding a new consumer requires Debezium changes
- ❌ No replay capability if consumer fails

#### Approach 2: Database Polling
```
Redis Consumer: SELECT * FROM db WHERE updated_at > last_check
```
**Problems**:
- ❌ Stale data (5-minute delay = eventual consistency lag)
- ❌ High database load (constant polling)
- ❌ Difficult to capture DELETE events

#### Approach 3: Message Queue (What We Chose)
```
Debezium → [Kafka Event Bus] → Redis Consumer
                            → Elasticsearch Consumer
                            → Analytics Consumer
```
**Benefits**:
- ✅ Debezium publishes ONCE (fast, no retries)
- ✅ Consumers read independently at their own pace
- ✅ Events persist; consumers can replay from any point
- ✅ Easy to add new consumers

---

## Alternatives Considered

### Alternative 1: Amazon Kinesis
**Pros**:
- ✅ AWS-managed (no ops overhead)
- ✅ Pay-per-shard (cost-effective at scale)

**Cons**:
- ❌ AWS lock-in (not learning portable skills)
- ❌ Local development requires LocalStack
- ❌ Less community support than Kafka
- ❌ More expensive for small scale

**Decision**: Rejected (out of scope for portfolio project)

---

### Alternative 2: RabbitMQ
**Pros**:
- ✅ Simpler setup than Kafka
- ✅ Lower latency

**Cons**:
- ❌ No ordering guarantees per queue
- ❌ Traditional message queue (not event stream)
- ❌ Doesn't support replay well
- ❌ Less suitable for CDC patterns

**Example problem**:
```
If Kafka loses ordering:
  Event 1: Product created with price=100
  Event 2: Price updated to 200
  
If consumed out of order:
  Event 2 processed first: SET price=200 (product doesn't exist yet!)
  Event 1 processed second: SET price=100 (WRONG!)

Kafka prevents this:
  Same product ID always goes to same partition
  Partitions preserve order
  Result: Always correct
```

**Decision**: Rejected (lacks ordering guarantees critical for CDC)

---

### Alternative 3: Redis Pub/Sub
**Pros**:
- ✅ Extremely simple
- ✅ Low latency

**Cons**:
- ❌ No persistence (events lost if no subscriber)
- ❌ No replay (can't catch up after crash)
- ❌ Limited to single server (not distributed)

**Example problem**:
```
T=10:00 Debezium publishes: {event: "product_created"}
T=10:01 Redis crashes
T=10:02 Redis recovers (event is GONE, no way to recover)
T=10:03 Elasticsearch Consumer has no idea what changed
        Result: Inconsistency!

Kafka solves this:
T=10:01 Kafka persists: {event: "product_created"}
T=10:02 Redis crashes
T=10:03 Elasticsearch Consumer reads from Kafka
T=10:04 Gets all events including the one from T=10:00
        Result: Consistent
```

**Decision**: Rejected (lacks durability for CDC)

---

### Alternative 4: Custom Event Storage (PostgreSQL Table)
**Pros**:
- ✅ Already using PostgreSQL
- ✅ Strong ACID guarantees

**Cons**:
- ❌ Single machine (scales poorly)
- ❌ Not designed for streaming (polling is inefficient)
- ❌ Tight coupling (consumers depend on DB)
- ❌ Operational nightmare (managing retention)

**Decision**: Rejected (not designed for event streaming)

---

## Decision

**We MUST use Apache Kafka as the event bus** because:

### Reason 1: Exactly-Once Semantics
```
Guarantee: "Every change is processed exactly once"

How Kafka enables this:
1. Events have sequence numbers (offsets)
2. Consumer tracks position (committed offset)
3. Consumer only processes events >= committed_offset
4. If consumer crashes, it knows where it left off

Example:
  Kafka offsets: 100, 101, 102, 103, 104
  Consumer processes: 100, 101, 102
  Consumer commits: offset=102
  Consumer crashes
  Consumer restarts
  Resume from: 103 (skips 100-102)
  Result: No duplicates
```

Why this matters for CDC:
- Financial systems: Duplicate transactions = fraud/compliance violation
- Inventory: Duplicate update = selling more than in stock
- Cache: Duplicate write = no visible problem, but wastes resources

---

### Reason 2: Ordering Guarantees
```
Guarantee: "All changes to a row are processed in order"

How Kafka provides ordering:
- Each table gets a topic: postgres.public.products
- Each topic has N partitions
- Each row (by ID) consistently goes to same partition via hash
- Within a partition, events are strictly ordered

Example:
  Topic: postgres.public.products (3 partitions)
  
  Row ID=1:  Always → Partition 0 → Events 1a, 1b, 1c (in order)
  Row ID=2:  Always → Partition 1 → Events 2a, 2b (in order)
  Row ID=3:  Always → Partition 2 → Events 3a (in order)
  
  Guarantees:
  - 1a happens before 1b happens before 1c
  - 2a happens before 2b
  - 3a stands alone
  
  BUT 1a and 2a can happen in any order (different partitions)
```

Why this matters for CDC:
- Products: INSERT → UPDATE → DELETE must happen in order
- Comments: CREATE → LIKE → DELETE must happen in order
- Without ordering: State machine violations

---

### Reason 3: Decoupling
```
With Kafka:
  Debezium produces once per change
  Redis consumer can be down, restarted, scaled independently
  Elasticsearch consumer can lag behind without affecting others
  Adding new consumer (Analytics) doesn't touch Debezium

Without Kafka (direct connections):
  Adding new consumer requires:
  - Modifying Debezium configuration
  - Restarting Debezium
  - Risk: downtime or lost events
```

---

### Reason 4: Scalability
```
If we publish 1,000 events/sec:

Without partitions:
  Single consumer processes all 1,000 events/sec
  Bottleneck: One consumer can't scale

With 3 partitions:
  Partition 0: ~333 events/sec
  Partition 1: ~333 events/sec
  Partition 2: ~334 events/sec
  
  Then:
  Partition 0: 2 consumers (166 each) via rebalancing
  Partition 1: 2 consumers (166 each)
  Partition 2: 1 consumer (334)
  Result: Horizontal scaling by adding consumers
```

---

### Reason 5: Durability & Replay
```
Kafka persists events to disk (configurable retention):
- Default: 7 days
- Can set to infinite (with cost)

Benefits:
1. If all consumers crash, events are still there
2. Can rebuild entire Redis cache from beginning
3. Can run analytics on historical data
4. Perfect for debugging ("replay from offset 1000")

Example debugging:
  Problem: Redis has stale data
  Action: Restart consumer with --from-offset=1000
  Result: Replays last 1,000 events, rebuilds cache
```

---

## Consequences

### Positive Consequences ✅

1. **Decoupled architecture** - Consumers are independent
2. **Scalability** - Add consumers/partitions without re-engineering
3. **Reliability** - Exactly-once delivery (if configured correctly)
4. **Debuggability** - Replay events from any point in time
5. **Observability** - Consumer lag = how far behind are we?

### Negative Consequences ❌

1. **Operational Complexity** - Another component to manage
   - Need to monitor Kafka cluster health
   - Need to manage retention
   - Need to manage replication factor

2. **Latency** - Small overhead compared to direct connections
   - Debezium → Kafka: ~30ms
   - Consumer reads from Kafka: ~100-200ms
   - Total: <500ms for most changes (acceptable)

3. **Cost** - Kafka cluster requires resources
   - Disk (events are persisted)
   - Memory (brokers, consumers)
   - CPU (coordination, replication)
   - Mitigation: Use Docker Compose for development, auto-scaling in production

4. **Learning Curve** - Team must understand:
   - Partitioning strategy
   - Consumer groups
   - Offset management
   - Exactly-once semantics
   - Mitigation: This project teaches all of this!

---

## Implementation Plan

### Phase 1: Setup
- [ ] Deploy Kafka locally with Docker Compose (3 brokers)
- [ ] Create topics: `postgres.public.{table_name}`
- [ ] Verify Debezium can publish to Kafka

### Phase 2: Consumers
- [ ] Build Redis consumer (learns consumer groups)
- [ ] Build Elasticsearch consumer
- [ ] Test exactly-once semantics (crash one consumer, verify replay)

### Phase 3: Monitoring
- [ ] Track consumer lag per partition
- [ ] Alert if lag exceeds threshold

### Phase 4: Production Hardening
- [ ] Tune retention policy
- [ ] Set replication factor=3
- [ ] Load test (1000+ events/sec)

---

## Related Decisions

- [ADR-002: Choice of Debezium](ADR-002-debezium.md) - Works with Kafka for output
- [ADR-003: Consumer Group Strategy](ADR-003-consumer-groups.md) - How consumers organize
- [ADR-004: Idempotent Consumers](ADR-004-idempotency.md) - Exactly-once implementation

---

## Questions for Reflection

1. **What would go wrong if we picked RabbitMQ instead?**
   - Think about a product that gets updated twice in succession
   - How would out-of-order updates break it?

2. **What if Kafka broker fails?**
   - Can consumers still read?
   - What's the recovery strategy?

3. **What if a consumer falls 1 day behind?**
   - How much disk space does Kafka need?
   - Should we adjust retention?

---

## References

- [Apache Kafka Design](https://kafka.apache.org/documentation/#design)
- [Kafka vs Other Message Queues](https://kafka.apache.org/intro)
- [Exactly-Once Semantics](https://kafka.apache.org/documentation/#consumerconfigs_isolation.level)

