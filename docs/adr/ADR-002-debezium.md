# ADR-002: Choice of Debezium for Change Data Capture

**Date**: June 2025  
**Status**: ✅ Accepted  
**Reviewers**: Architecture Team  
**Relevant to Phase**: 0 (Architecture)

---

## Summary

**Decision**: Use Debezium as the CDC engine to capture PostgreSQL changes and publish to Kafka.

**Key Rationale**:
- ✅ Battle-tested, widely adopted (used by companies like Shopify, Segment, Grab)
- ✅ Abstracts WAL complexity (PostgreSQL's internals are hard to work with directly)
- ✅ Handles schema evolution (adds/removes columns)
- ✅ Built-in monitoring and dead letter queues
- ✅ Supports multiple databases (MySQL, MongoDB, Oracle, etc.)

---

## Context: What Is CDC?

### The Core Problem
PostgreSQL's Write-Ahead Log (WAL) is powerful but complex:
```
Raw WAL Segment (binary data):
[01 10 AF 88] [02 AA EE FF] [03 BB DD 99] ...
           LSN: 0/1A230000  INSERT
           LSN: 0/1A230050  UPDATE
           LSN: 0/1A2300A0  DELETE
```

**Challenge**: 
- Reading WAL requires PostgreSQL internal knowledge
- Handling restarts, resuming from checkpoints is tricky
- Parsing binary format is error-prone
- Managing replication slots manually is operational burden

### The Solution: Abstraction Layer
```
Raw WAL (binary)
    ↓
Debezium (decoder)
    ↓
Structured Events (JSON)
    ↓
Kafka Topics
```

Debezium handles:
- ✅ Binary WAL parsing
- ✅ PostgreSQL replication slots
- ✅ Event serialization
- ✅ Error recovery

---

## Alternatives Considered

### Alternative 1: PostgreSQL Logical Replication (Built-In)
```
PostgreSQL has native "Logical Decoding" feature:

CREATE PUBLICATION my_publication FOR ALL TABLES;
SELECT * FROM pg_logical_slot_get_changes('my_slot', NULL, NULL);
```

**Pros**:
- ✅ No external tool (less operational complexity)
- ✅ Lower latency (in-process)
- ✅ Built-in feature (PostgreSQL maintains it)

**Cons**:
- ❌ Limited output formats (only replication stream, not Kafka)
- ❌ Single-machine replication (no distributed consumption)
- ❌ Less mature ecosystem
- ❌ Custom logic needed for Kafka publishing
- ❌ Harder to scale consumers

**Example limitation**:
```
With Logical Replication:
  PostgreSQL → (custom code) → Kafka

With Debezium:
  PostgreSQL → Debezium → Kafka

If we add Elasticsearch:
  With Logical Replication:
    Custom code must write to BOTH Kafka and ES
    (or Kafka first, then ES consumer)
  
  With Debezium:
    Debezium → Kafka → ES Consumer (automatic)
```

**Decision**: Rejected (lacks Kafka integration, single-machine replication)

---

### Alternative 2: Build CDC from Scratch
```
Custom code that:
1. Reads PostgreSQL WAL files
2. Parses binary WAL format
3. Publishes to Kafka
4. Handles restarts and checkpoints
```

**Pros**:
- ✅ Complete understanding of every line
- ✅ Optimized for our specific use case

**Cons**:
- ❌ 3-6 months of development
- ❌ Edge cases: schema changes, DDL statements, etc.
- ❌ Maintenance burden (PostgreSQL updates WAL format)
- ❌ Testing nightmare (crash scenarios, corruption)
- ❌ Non-transferable skill (Debezium is industry standard)

**Hidden complexity**:
```
What happens when:
- PostgreSQL gets updated (new WAL version)?
- Schema changes (add/remove columns)?
- A constraint violation occurs?
- A transaction rolls back?
- A WAL segment gets corrupted?
- 1,000 events/sec hit the pipeline?

Debezium handles all of these. We'd have to handle all of these.
```

**Decision**: Rejected (impractical, non-portable learning)

---

### Alternative 3: AWS Database Migration Service (DMS)
```
AWS DMS:
- Managed CDC service
- Works with PostgreSQL, MySQL, Oracle
- Outputs to S3, Kinesis, or Kafka
```

**Pros**:
- ✅ Fully managed (no ops)
- ✅ Built-in monitoring

**Cons**:
- ❌ AWS-specific (not portable)
- ❌ Expensive for small deployments
- ❌ Locked into AWS ecosystem
- ❌ Can't run locally for learning

**Decision**: Rejected (out of scope; doesn't teach architectural concepts)

---

### Alternative 4: MySQL Binlog-Based CDC
```
If we used MySQL instead of PostgreSQL:
  MySQL → MySQL Binlog → Debezium → Kafka
```

**Why not MySQL**:
- PostgreSQL's WAL is more reliable and consistent
- PostgreSQL's replication slots are cleaner
- Debezium+PostgreSQL is the gold standard for CDC
- Learning PostgreSQL+Debezium is more valuable

**Decision**: Not considered seriously (PostgreSQL is better choice)

---

## Decision

**We MUST use Debezium** because:

### Reason 1: Battle-Tested Reliability
```
Companies using Debezium in production:
- Shopify (100k+ merchants, millions of events/sec)
- Segment (data integration platform)
- Grab (Southeast Asia ride-hailing, real-time analytics)
- Kafka's own CDC examples use Debezium

This means:
- Edge cases have been discovered and fixed
- Community support is strong
- Documentation is comprehensive
- Job market values this skill
```

**Why reliability matters**:
```
If CDC loses or duplicates events:
- Inventory: Overselling (missed DELETE)
- Cache: Showing old prices (missed UPDATE)
- Analytics: Incorrect KPIs (duplicates)
- Trust: System unreliable, need to rebuild from scratch

Debezium's maturity reduces this risk significantly.
```

---

### Reason 2: Abstracts WAL Complexity
```
PostgreSQL WAL is binary:

Hex dump of a real WAL segment:
00000000: 0000 0004 0000 0000 aaaa aaaa 0000 0001
00000010: 0000 2000 0000 0000 aaaa aaaa 0000 0000
00000020: 0004 0028 0000 0000 aaaa aaaa ...

What does it mean?
- Version info?
- Transaction ID?
- Timestamp?
- Actual data?

Debezium handles this complexity:
PostgreSQL WAL (binary) → Debezium → JSON Event
{
  "op": "c",
  "table": "products",
  "after": {"id": 1, "name": "Laptop"},
  "ts_ms": 1623859200000
}

Now we can reason about events, not hex values.
```

---

### Reason 3: Schema Evolution Support
```
Real databases change over time:

T=10:00 Schema: products(id, name, price)
T=10:15 ALTER TABLE products ADD COLUMN stock_quantity INT;
T=10:30 Event published with new column
T=11:00 Event published without new column (different row)

Without CDC abstraction:
- WAL format changes
- Consumer code breaks
- Need manual migration

With Debezium:
- Detects schema change
- Includes schema version in events
- Consumer can handle both old and new schemas
```

Example:
```
Debezium event with schema evolution:
{
  "op": "c",
  "table": "products",
  "schema": {
    "version": 1,
    "columns": ["id", "name", "price", "stock_quantity"]
  },
  "after": {"id": 1, "name": "Laptop", "price": 999.99, "stock_quantity": 10}
}

Consumer reads schema first, then deserializes data correctly.
```

---

### Reason 4: Built-In Monitoring & Error Handling
```
Debezium provides:
- Offset tracking (how many events processed?)
- Snapshot mode (initial full table load before CDC)
- Dead letter queues (events that fail processing)
- Heartbeats (detects stalled connectors)
- Metrics (events/sec, latency, etc.)

Building this ourselves:
- At least 2-3 weeks of additional work
- Debugging is harder (less community knowledge)
```

---

### Reason 5: Transferable Knowledge
```
Learning Debezium teaches:
- CDC concepts (applicable to any database)
- PostgreSQL replication mechanics
- Kafka connector framework
- Data pipeline patterns

These skills transfer to:
- MySQL CDC with Debezium
- MongoDB change streams with custom consumer
- Oracle CDC (also uses Debezium)

Learning to build CDC from scratch teaches:
- PostgreSQL WAL format (specific knowledge)
- Binary parsing (niche skill)
- Limited transferability
```

---

## Consequences

### Positive Consequences ✅

1. **Reliability** - Proven, used at scale
2. **Development speed** - Don't reinvent the wheel
3. **Maintenance** - Debezium team handles updates
4. **Learning quality** - Understand architecture, not binaries
5. **Troubleshooting** - Large community, many StackOverflow answers
6. **Extensibility** - Debezium has transformations, filters, etc.

### Negative Consequences ❌

1. **Operational Complexity** - Another component to manage
   - Debezium process must stay healthy
   - Logs must be monitored
   - Resources (CPU, memory) must be provisioned

2. **Learning Curve** - Must understand Debezium concepts
   - Snapshots vs. incremental
   - Replication slots
   - Connector configuration
   - Transformation rules
   - Mitigation: This project teaches all of it

3. **Latency Overhead** - Debezium adds ~30-50ms
   - WAL → Debezium: parsing, serialization
   - Not significant for most use cases

4. **Vendor Lock (Soft)** - Debezium-specific patterns
   - Connector config (YAML/JSON)
   - Transformation syntax
   - Offset management
   - Mitigation: Skills are portable to other Kafka connectors

---

## How Debezium Works (Simplified)

### Step 1: PostgreSQL Replication Slot
```
Debezium tells PostgreSQL:
  "Reserve WAL segments for me, I want to read from LSN 0/1A000000"

PostgreSQL:
  - Creates replication slot named "debezium"
  - Prevents WAL cleanup while slot is active
  - Advances slot position as Debezium catches up
```

### Step 2: WAL Reading
```
Debezium:
  - Connects to PostgreSQL as replication client
  - Starts reading from LSN 0/1A000000
  - Receives: INSERT, UPDATE, DELETE events
  - Receives: DDL statements (CREATE TABLE, ALTER TABLE)
```

### Step 3: Event Transformation
```
Raw WAL:
  LSN 0/1A230150: INSERT INTO products (id, name) VALUES (1, 'Laptop')

Debezium:
  Parses binary → Understands structure
  Converts to JSON:
  {
    "op": "c",
    "table": "products",
    "database": "shop",
    "schema": "public",
    "before": null,
    "after": {"id": 1, "name": "Laptop"},
    "source": {
      "version": "1.7.0",
      "connector": "postgresql",
      "name": "postgres-cdc",
      "ts_ms": 1623859200000,
      "lsn": 394686968,
      "xmin": null,
      "txId": 12345,
      "snapshot": false,
      "sequence": null,
      "table": "products"
    }
  }
```

### Step 4: Publish to Kafka
```
Debezium publishes:
- Topic: postgres.public.products
- Key: {"id": 1}  (for partitioning)
- Value: (above JSON)
- Headers: {"source.snapshot": false}

Kafka:
  - Stores event durably
  - Assigns to partition (hash(key) % partitions)
  - Replicates to followers
  - Acknowledges to Debezium
```

### Step 5: Offset Management
```
Debezium tracks:
  "I successfully published events up to LSN 0/1A230150"
  
Stored in Kafka (topic: _debezium-offset-topic):
  {
    "connector": "postgres-cdc",
    "partition": "0",
    "offset": {"lsn": 394686968, "txId": 12345}
  }

On restart:
  Debezium reads last committed offset
  Resumes from LSN 0/1A230150
  Prevents duplicate events
```

---

## Implementation Plan

### Phase 1: Setup (Week 1)
- [ ] Docker Compose configuration for PostgreSQL + Debezium
- [ ] Create replication slot
- [ ] Test basic snapshot

### Phase 2: Configuration (Week 1-2)
- [ ] Create Kafka topics for each table
- [ ] Configure table.include.list
- [ ] Set up snapshot mode
- [ ] Test first CDC event

### Phase 3: Monitoring (Week 2)
- [ ] Monitor Debezium connector status
- [ ] Track LSN position (how far through WAL?)
- [ ] Verify events in Kafka

### Phase 4: Hardening (Week 3+)
- [ ] Handle schema changes
- [ ] Test failure scenarios (PostgreSQL restarts)
- [ ] Optimize replication slot settings

---

## Questions for Reflection

1. **What happens if PostgreSQL adds a new column to the products table?**
   - Does Debezium detect it?
   - How do existing consumers handle the new column?
   - Do we need to update consumer code?

2. **What if Debezium falls 1 hour behind on processing WAL?**
   - How much disk space does PostgreSQL need?
   - Should we trigger an alert?
   - Can we recover?

3. **How does Debezium handle transactions?**
   - If a transaction inserts 1,000 rows, do we get 1,000 events or 1 batched event?
   - Does ordering matter across events?

4. **How would you migrate from Debezium to AWS DMS?**
   - What would break?
   - How would you handle the transition?

---

## Related Decisions

- [ADR-001: Choice of Kafka](ADR-001-kafka.md) - Debezium outputs to Kafka
- [ADR-003: Consumer Group Strategy](ADR-003-consumer-groups.md) - Consumers read Kafka
- [ADR-005: Schema Evolution](ADR-005-schema-evolution.md) - How to handle schema changes

---

## References

- [Debezium Official Docs](https://debezium.io/)
- [PostgreSQL Replication Slot Docs](https://www.postgresql.org/docs/current/warm-standby.html#STREAMING-REPLICATION-SLOTS)
- [CDC Patterns](https://martinfowler.com/articles/patterns-of-distributed-systems/wal.html)

