# SyncStream: Architecture Overview

## Part 1: High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SyncStream Platform                          │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────┐
│   PostgreSQL         │
│   (Source DB)        │
│                      │
│  Write-Ahead Log     │
│  (WAL)               │
└──────────────┬───────┘
               │
               │ (Real-time change events)
               ↓
┌──────────────────────────────────────────────────┐
│         CDC Layer: Debezium Connector             │
│  ┌────────────────────────────────────────────┐  │
│  │ WAL Reader → Change Capture → Kafka Event  │  │
│  │              Publisher                      │  │
│  └────────────────────────────────────────────┘  │
└──────────────┬───────────────────────────────────┘
               │
               │ (Ordered change events)
               ↓
┌──────────────────────────────────────────────────────────────────┐
│          Apache Kafka Cluster (Event Bus)                         │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │ postgres.public.products (Partition 0, 1, 2)             │   │
│  │ postgres.public.customers (Partition 0, 1)               │   │
│  │ postgres.public.orders (Partition 0, 1, 2)               │   │
│  └───────────────────────────────────────────────────────────┘   │
└──────────────┬──────────────────┬──────────────────┬──────────────┘
               │                  │                  │
               ↓                  ↓                  ↓
   ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
   │  Redis Consumer  │ │    Elasticsearch │ │   Analytics      │
   │                  │ │    Consumer      │ │   Consumer       │
   │  (Cache Sync)    │ │                  │ │ (Data Warehouse) │
   │                  │ │  (Index Sync)    │ │                  │
   └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
            │                    │                    │
            ↓                    ↓                    ↓
   ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
   │      Redis       │ │  Elasticsearch   │ │  Analytics DB    │
   │      Cache       │ │   (Search)       │ │  (Data Lake)     │
   └──────────────────┘ └──────────────────┘ └──────────────────┘
```

---

## Part 2: Core Components Explained

### Component 1: PostgreSQL (Source of Truth)

**What It Is**:
The primary database containing all business data.

**Why CDC Works Here**:
- PostgreSQL Write-Ahead Log (WAL) records EVERY change before it's applied
- This creates a perfect audit trail of what changed, when, and by whom

**Key Concepts**:
```
Write-Ahead Log (WAL):
- Before PostgreSQL writes to disk, it writes to the WAL
- This ensures durability (even if server crashes, WAL survives)
- CDC tools read the WAL to capture changes

Example WAL Entry:
  LSN 0/1A230000: INSERT INTO products VALUES (123, 'Laptop', 999.99)
  LSN 0/1A230100: UPDATE products SET price=899.99 WHERE id=123
  LSN 0/1A230200: DELETE FROM products WHERE id=123
```

**Enterprise Considerations**:
- WAL files grow (need to manage retention: usually keep 30 days of history)
- Replication slot in PostgreSQL reserves WAL for CDC (prevents cleanup)
- If CDC consumer falls behind, WAL disk space can fill up

---

### Component 2: Debezium CDC Connector

**What It Is**:
A framework that reads PostgreSQL's WAL and translates raw changes into **events**.

**Why We Need It** (vs. Building From Scratch):
| Approach | Pros | Cons |
|----------|------|------|
| **Build CDC from scratch** | Learn everything | 6 months of work, many edge cases |
| **Use Debezium** | Battle-tested, supports 15+ databases | Need to understand its design |
| **Use native tools** (logical replication) | Built-in | Less flexible, harder to scale |

**What Debezium Does**:
```
Raw WAL Entry:
  LSN 0/1A230000, INSERT INTO products (id, name, price) VALUES (123, 'Laptop', 999.99)

↓ Debezium transforms to:

Structured Event:
{
  "op": "c",                                    // 'c'reate (INSERT)
  "ts_ms": 1623859200000,                       // timestamp
  "table": "products",
  "schema": "public",
  "before": null,                               // old values (null for INSERT)
  "after": {
    "id": 123,
    "name": "Laptop",
    "price": 999.99
  }
}
```

**Key Insight**:
Debezium reads from replication slots (PostgreSQL feature that preserves WAL). It doesn't interfere with normal database operations.

**Enterprise Considerations**:
- Single point of failure (what if Debezium crashes?) → Fixed with Kafka Connect distributed mode
- Schema changes (add/remove columns) → Debezium includes schema in events

---

### Component 3: Apache Kafka (Event Bus)

**What It Is**:
A distributed message broker that:
- Stores events durably (on disk)
- Allows multiple consumers to read the same events
- Preserves ordering per partition

**Why We Need Kafka**:

| Problem | How Kafka Solves It |
|---------|-------------------|
| **Decoupling** | Debezium publishes once; Redis, Elasticsearch, Analytics all consume independently |
| **Durability** | If Redis consumer crashes, Kafka replays events from last offset |
| **Ordering** | Events for a table are ordered (important for state consistency) |
| **Scaling** | Add consumers without touching Debezium |
| **Backpressure** | Slow consumer doesn't block Debezium |

**How Kafka Organizes Events**:

```
Topic: postgres.public.products (3 partitions for parallelism)

Partition 0: [Event-001: INSERT {id: 1}]
             [Event-003: UPDATE {id: 3}]
             [Event-005: DELETE {id: 5}]

Partition 1: [Event-002: INSERT {id: 2}]
             [Event-004: UPDATE {id: 2}]

Partition 2: [Event-006: INSERT {id: 6}]
```

**Ordering Guarantee**:
- Events for the SAME row stay together (same partition)
- Example: product id=1's INSERT, UPDATE, DELETE all go to partition 0
- This ensures we don't apply UPDATE before INSERT

**Key Challenge**:
- Partitioning is by row key (id)
- Debezium hashes the key to decide partition
- All changes for id=123 go to same partition (preserves causality)

**Enterprise Considerations**:
- Retention: How long to keep events? (7 days? 30 days? forever?)
- Replication: Events replicated to 3 brokers (fault tolerance)
- Exactly-once: Kafka guarantees delivery, consumers must guarantee idempotency

---

### Component 4: Redis Consumer

**What It Is**:
A Java service that reads from Kafka and updates Redis.

**Why We Build This**:
- Understand consumer group mechanics
- Learn idempotent updates (critical for exactly-once semantics)
- Handle failures gracefully

**Flow**:
```
Kafka Event: {op: "c", table: "products", after: {id: 123, name: "Laptop"}}
  ↓
Redis Consumer reads
  ↓
Deserialize event (JSON → Java object)
  ↓
Idempotent update: SET product:123 {name: "Laptop", ...}
  ↓
Commit offset to Kafka (mark event as processed)
```

**Idempotency Explained**:

```
Scenario: Event is processed twice (network retry)

Without idempotency:
  Process 1: SET product:123 name="Laptop"
  Process 2: SET product:123 name="Laptop"  (duplicate!)
  Problem: How do we know it was processed? Did it succeed?

With idempotency (what we'll build):
  Process 1: SET product:123 {name: "Laptop", version: 1}
  Process 2: SET product:123 {name: "Laptop", version: 1}  (ignored, same version)
  Result: Redis has exactly one update, not two
```

**Enterprise Considerations**:
- Consumer lag monitoring: "How far behind are we?"
- Rebalancing: When a consumer dies, others take over its partitions
- Circuit breakers: If Redis is down, pause consumption (don't lose events)

---

### Component 5: Elasticsearch Consumer

**What It Is**:
Similar to Redis consumer, but publishes to Elasticsearch for full-text search.

**Key Differences**:
- Creates/updates documents in indices
- Supports complex mappings (field types, analyzers)
- Handles index refreshes

**Flow**:
```
Kafka Event: {op: "u", table: "products", after: {id: 123, name: "Gaming Laptop"}}
  ↓
Transform to Elasticsearch document:
  {
    "id": 123,
    "name": "Gaming Laptop",
    "timestamp": "2025-06-16T10:00:00Z",
    "@version": 1
  }
  ↓
PUT /products/_doc/123 (index the document)
  ↓
Commit offset
```

---

### Component 6: Analytics Consumer

**What It Is**:
Exports events to an analytics warehouse for reporting/BI.

**Differences from Redis/Elasticsearch**:
- Append-only (don't delete rows)
- Batch processing (not real-time)
- Focus on data lineage (who changed what, when?)

---

## Part 3: Data Flow Scenarios

### Scenario 1: INSERT (New Product)
```
1. Admin adds product via web app
   INSERT INTO products VALUES (789, 'Monitor', 299.99)

2. PostgreSQL writes to WAL
   WAL: LSN 0/1A230150, INSERT INTO products (789, 'Monitor', 299.99)

3. Debezium reads from WAL
   Converts to: {op: "c", table: "products", after: {id: 789, name: "Monitor", price: 299.99}}

4. Debezium publishes to Kafka
   Topic: postgres.public.products
   Partition: hash(789) mod 3 = partition 0
   Key: 789
   Value: {op: "c", ...}

5. Redis Consumer reads from partition 0
   Deserializes event
   Executes: SET product:789 {id: 789, name: "Monitor", price: 299.99}

6. Elasticsearch Consumer reads same event
   Executes: PUT /products/_doc/789 {id: 789, name: "Monitor", ...}

7. Analytics Consumer batches the event
   Appends to warehouse: INSERT INTO analytics.products_changelog VALUES (789, 'Monitor', 'INSERT', timestamp)

8. Result:
   ✅ PostgreSQL: products table has new row
   ✅ Redis: product:789 key exists
   ✅ Elasticsearch: Document indexed, searchable
   ✅ Analytics: Change recorded
```

**Latency Breakdown**:
- WAL → Debezium: ~50ms
- Debezium → Kafka: ~30ms
- Kafka → Consumer: ~200ms
- Consumer → Redis: ~100ms
- **Total**: ~380ms (within <1 second SLA)

---

### Scenario 2: UPDATE (Price Change)
```
1. Admin updates product price
   UPDATE products SET price=249.99 WHERE id=789

2. WAL entry
   LSN 0/1A230200, UPDATE products SET price=249.99 WHERE id=789

3. Debezium Event
   {
     op: "u",                           // Update
     before: {id: 789, name: "Monitor", price: 299.99},
     after: {id: 789, name: "Monitor", price: 249.99}
   }

4. Kafka publishes to partition 0 (same as INSERT, key=789)

5. Redis Consumer
   UPDATE: SET product:789 {id: 789, name: "Monitor", price: 249.99}
   (Overwrites old value, idempotent)

6. Result:
   ✅ Cache has new price
   ✅ Elasticsearch reindexed with new price
   ✅ Analytics sees price change
```

**Why Order Matters**:
```
Bad ordering scenario:
  Event 1 (t=10:00): UPDATE price=299.99
  Event 2 (t=10:05): UPDATE price=249.99
  But consumer processes them:
    Process Event 2 first (oops!)
    Result: price=249.99
    Process Event 1 (overwrite!)
    Final: price=299.99 (WRONG! Should be 249.99)

Kafka prevents this:
  - Same row (id=789) always goes to same partition
  - Events in partition are ordered by offset
  - Consumer processes in order
  - Result: Always correct
```

---

### Scenario 3: DELETE (Remove Product)
```
1. Admin deletes product
   DELETE FROM products WHERE id=789

2. Debezium Event
   {
     op: "d",                           // Delete
     before: {id: 789, name: "Monitor", price: 249.99},
     after: null                        // No "after" for delete
   }

3. Redis Consumer
   DEL product:789

4. Elasticsearch Consumer
   DELETE /products/_doc/789

5. Analytics Consumer
   INSERT INTO products_changelog VALUES (789, 'Monitor', 'DELETE', timestamp)

6. Result:
   ✅ Cache key removed
   ✅ Search index updated
   ✅ Audit trail recorded
```

---

## Part 4: Failure Scenarios & Resilience

### Failure 1: Redis Temporarily Down
```
Timeline:
  T=10:00 Event 1 published to Kafka
  T=10:01 Event 2 published to Kafka
  T=10:02 Redis Consumer tries to connect → Redis Down!
          Consumer pauses, doesn't commit offset
  T=10:05 Redis comes back
  T=10:06 Consumer reconnects, reads from last committed offset
          Reprocesses Event 1 & 2
          Idempotent updates ensure no duplicates
          ✅ Consistent

Why Idempotency Matters:
  Without: We might increment a counter twice, creating inconsistency
  With: SET product:789 {version: 1} is idempotent (same result each time)
```

### Failure 2: Consumer Crashes
```
Timeline:
  T=10:00 Redis Consumer-1 processing events
          Processes offset 100
          Crashes before committing
  T=10:01 Kafka detects consumer is dead
  T=10:02 Another instance (Consumer-2) joins group
  T=10:03 Kafka reassigns Consumer-1's partitions to Consumer-2
  T=10:04 Consumer-2 reads from last committed offset (99)
          Reprocesses events 100+
          ✅ No data loss

This is Consumer Group Rebalancing:
  - Automatic failure detection
  - Automatic work redistribution
  - Exactly-once delivery (with idempotent consumer)
```

### Failure 3: Debezium Crashes
```
Timeline:
  T=10:00 Debezium processing WAL up to LSN 0/1A230500
  T=10:05 Debezium crashes
  T=10:10 Debezium restarts
  T=10:11 Debezium reconnects to PostgreSQL replication slot
          Reads from LSN 0/1A230500 (where it left off)
          Publishes missing events to Kafka
          ✅ No loss

PostgreSQL Replication Slots:
  - Reserve WAL segments on disk
  - Allow CDC to resume from checkpoint
  - Prevent WAL from being deleted while CDC is behind
```

---

## Part 5: Technology Choices & Trade-offs

### Choice 1: PostgreSQL for Source
**Alternatives Considered**:
- MySQL: Supports CDC (via binlog), but less reliable
- MongoDB: Supports change streams, but different mental model
- Oracle: Supports CDC, but enterprise-only

**Why PostgreSQL**:
- ✅ Strong WAL design (perfect for CDC)
- ✅ Free/open-source
- ✅ Replication slots built-in
- ✅ Debezium fully supports it
- ✅ Industry standard (most startups use it)

---

### Choice 2: Debezium vs. Logical Decoding
**Debezium**:
- External tool, runs separately
- Flexible (works with Kafka, S3, any output)
- Mature, battle-tested

**PostgreSQL Logical Decoding** (native):
- Built-in feature
- Lower latency
- Limited flexibility (only replication, not stream)

**Why Debezium**:
- ✅ Teaches CDC architecture (applicable to other databases)
- ✅ Kafka output is useful (all consumers read from one place)
- ✅ Decouples CDC from consumer logic
- ❌ Extra component to manage

---

### Choice 3: Kafka vs. Pulsar/RabbitMQ
**Kafka**:
- Ordered partitions ✅
- Partition rebalancing ✅
- Exactly-once (transactional) ✅
- Widely used ✅

**Pulsar**:
- Better multi-datacenter ❌ (out of scope)
- Geo-replication ❌ (out of scope)

**RabbitMQ**:
- No ordering guarantees ❌

**Why Kafka**:
- ✅ Purpose-built for event streaming
- ✅ Designed for CDC use cases
- ✅ Ordering per partition
- ✅ LinkedIn, Netflix, Stripe use it at scale

---

### Choice 4: Redis vs. Memcached
**Redis**:
- Rich data structures (hashes, sets, sorted sets) ✅
- Persistence options ✅
- Lua scripting (atomic operations) ✅

**Memcached**:
- Simpler, faster
- No persistence

**Why Redis**:
- ✅ Idempotent updates (SET is idempotent)
- ✅ More features (we'll use hashes for structured data)
- ✅ Industry standard (most startups)

---

### Choice 5: Elasticsearch vs. Solr
**Elasticsearch**:
- Full-text search ✅
- Distributed ✅
- RESTful API ✅
- Widely adopted ✅

**Solr**:
- More powerful search
- More complex

**Why Elasticsearch**:
- ✅ Industry standard for search
- ✅ Easy to set up locally
- ✅ Great for learning (common in interviews)

---

## Part 6: Evolution Path

**Phase 1-2**: Single Kafka broker, single consumer instances
- Learn fundamentals
- Understand failure modes

**Phase 3-4**: Multiple Kafka brokers, consumer groups
- Learn distributed failure handling
- Understand rebalancing

**Phase 5-6**: Multi-consumer, batching, analytics pipeline
- Learn back-pressure handling
- Scale to multiple consumers

---

## Next Steps

→ Move to [Data Flow Diagrams](DATA_FLOW.md) for detailed sequence diagrams
→ Then read [Architecture Decision Records](../adr/ADR-001-kafka.md) to understand WHY we chose Kafka

