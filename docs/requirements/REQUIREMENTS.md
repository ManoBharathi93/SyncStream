# SyncStream: Functional & Non-Functional Requirements

## Part 1: Functional Requirements

These define **WHAT** the system does.

### FR-1: Change Capture
**Description**: Capture all changes (INSERT, UPDATE, DELETE) from PostgreSQL

**Details**:
- Monitor PostgreSQL Write-Ahead Log (WAL)
- Detect when rows are inserted, updated, or deleted
- Capture the row's new state (and old state for updates)
- Include metadata: timestamp, user, table name

**Example**:
```
Event 1: INSERT into products
  {id: 123, name: "Laptop", price: 999.99, timestamp: 2025-06-16T10:00:00Z}

Event 2: UPDATE products SET price=899.99 WHERE id=123
  {id: 123, old_price: 999.99, new_price: 899.99, timestamp: 2025-06-16T10:05:00Z}

Event 3: DELETE from products WHERE id=123
  {id: 123, timestamp: 2025-06-16T10:10:00Z}
```

**Acceptance Criteria**:
- ✅ All INSERTs are captured
- ✅ All UPDATEs are captured (with before/after values)
- ✅ All DELETEs are captured
- ✅ No changes are missed (exactly-once delivery)

**Why This Matters**:
- If we miss a change, downstream systems become inconsistent
- Applications make decisions based on incomplete data
- Financial systems would violate compliance

---

### FR-2: Event Publishing to Kafka
**Description**: Publish captured changes to Apache Kafka for async distribution

**Details**:
- For each change, publish to a Kafka topic (e.g., `postgres.public.products`)
- Each topic corresponds to a table
- Events are ordered by transaction time

**Example Topic Structure**:
```
Topic: postgres.public.products
Partition 0:
  [Event-001: INSERT {id: 1, ...}]
  [Event-002: UPDATE {id: 1, ...}]
  [Event-003: DELETE {id: 1, ...}]

Topic: postgres.public.customers
  [Event-001: INSERT {customer_id: 100, ...}]
  ...
```

**Acceptance Criteria**:
- ✅ Each table change publishes to a dedicated topic
- ✅ Events in a topic are ordered (by transaction ID)
- ✅ Events are available to consumers immediately (no batch delays)

**Why This Matters**:
- Kafka is the "contract" between CDC and consumers
- Ordering ensures we can rebuild state correctly
- Async publishing decouples source from sinks

---

### FR-3: Redis Real-Time Cache Sync
**Description**: Keep Redis in sync with PostgreSQL in real-time

**Details**:
- Kafka consumer listens to product change events
- For INSERT/UPDATE: Update Redis key `product:{id}` with JSON
- For DELETE: Remove Redis key `product:{id}`

**Example**:
```
PostgreSQL: INSERT product(id=1, name="Laptop")
  ↓ CDC captures
Kafka: Event published
  ↓ Redis consumer reads
Redis: SET product:1 {"id": 1, "name": "Laptop"}
```

**Acceptance Criteria**:
- ✅ INSERT → Redis key created within 1 second
- ✅ UPDATE → Redis key updated within 1 second
- ✅ DELETE → Redis key removed within 1 second
- ✅ Redis and PostgreSQL never diverge (idempotent updates)

**Why This Matters**:
- Real-time cache is critical for user-facing features (profile pages, recommendations)
- <1 second consistency prevents showing stale data

---

### FR-4: Elasticsearch Search Index Sync
**Description**: Keep Elasticsearch indices in sync with PostgreSQL

**Details**:
- Similar to Redis, but for full-text search indexing
- Publish structured documents to Elasticsearch indices
- Support index refreshes and field mappings

**Acceptance Criteria**:
- ✅ Documents indexed in Elasticsearch within 1 second
- ✅ Full-text search works on indexed fields
- ✅ Index mappings don't break on schema changes

**Why This Matters**:
- Users expect instant search (e.g., Google-like)
- Without CDC, would need batch re-indexing (stale results)

---

### FR-5: Analytics Export
**Description**: Export changes to an analytics warehouse

**Details**:
- Collect events into batches
- Export to Analytics system (or flat file for MVP)
- Include change history (for trend analysis)

**Acceptance Criteria**:
- ✅ Events are batched efficiently
- ✅ No data loss during export
- ✅ Analysts can query change history

---

## Part 2: Non-Functional Requirements

These define **HOW WELL** the system does it.

### NFR-1: Reliability & Exactness
**Requirement**: Exactly-once event delivery (no duplicates, no loss)

**Why This Matters**:
- Financial transactions: duplicate charges = compliance violation
- Inventory: missed updates = overselling
- Cache consistency: duplicates = stale data

**How We'll Achieve It**:
- Kafka provides durable ordering
- Consumers track offsets and write-once
- Idempotent operations (overwrite, not append)

**Testing**:
- Simulate broker failures
- Verify no events are lost or duplicated

---

### NFR-2: Latency
**Requirement**: Changes reach downstream systems within 1 second

**Why This Matters**:
- E-commerce: "Add to cart" should update inventory immediately
- Fraud detection: Unusual transaction flagged in real-time
- User experience: Profile updates visible instantly

**Acceptable Latencies**:
- PostgreSQL → Kafka: <100ms
- Kafka → Consumer: <500ms
- Consumer → Redis/ES: <200ms
- **Total**: <1 second

**Monitoring**:
- Track end-to-end latency with timestamps
- Alert if P99 latency exceeds 2 seconds

---

### NFR-3: Scalability
**Requirement**: Support growing data volumes without re-architecture

**Baseline**:
- 1,000 writes/second to PostgreSQL
- Grow to 10,000 writes/second

**How We'll Achieve It**:
- Kafka partitioning (horizontal scaling)
- Consumer group scaling
- Connection pooling

---

### NFR-4: Fault Tolerance
**Requirement**: System continues operating if components fail

**Failure Scenarios**:
1. PostgreSQL briefly unavailable → CDC resumes from last offset
2. Kafka broker fails → Replication kicks in
3. Consumer crashes → Another consumer in group takes over
4. Network partition → Graceful degradation, no data loss

**Not Required (Out of Scope)**:
- Recovery from complete Kafka cluster loss (multi-DC replication is enterprise-only)

---

### NFR-5: Schema Evolution
**Requirement**: Support database schema changes without breaking consumers

**Scenarios**:
1. Add new column to table → Old consumers still work
2. Rename column → Consumers can handle both names
3. Change column type → Graceful migration

**Why This Matters**:
- Real databases evolve constantly (migrations)
- Consumers can't all redeploy simultaneously

---

### NFR-6: Observability
**Requirement**: Understand what the system is doing

**Monitoring**:
- How many events per second?
- What's the consumer lag (how far behind Kafka)?
- What errors occurred?

**Logging**:
- Structured JSON logs (not text logs)
- Include event IDs for tracing

**Not Required**: Full APM (like DataDog), Prometheus metrics (we'll add basic logging)

---

### NFR-7: Data Privacy & Security (Awareness)
**Requirement**: Recognize data sensitivity

**Current Scope**: We'll *not* implement encryption/masking in MVP

**Why We Document It**:
- Real systems must anonymize PII (credit cards, SSNs)
- Elasticsearch leaks data if misconfigured
- CDC logs reveal every change (audit issue)

**We'll**: Document the risks, plan for Phase 6

---

## Part 3: Use Case Acceptance Criteria

### Use Case 1: Product Inventory Sync
**Scenario**: E-commerce warehouse

**Given**: Product table in PostgreSQL
```sql
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  price DECIMAL(10, 2),
  stock_quantity INT
);
```

**When**: Admin updates stock
```sql
UPDATE products SET stock_quantity = 5 WHERE id = 123;
```

**Then**:
- ✅ Redis is updated: `product:123:stock = 5`
- ✅ Elasticsearch index is refreshed
- ✅ Analytics sees the change
- ✅ All within 1 second

---

### Use Case 2: Customer Profile Search
**Scenario**: Social network

**Given**: Customers table
```sql
CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255),
  name VARCHAR(255),
  bio TEXT
);
```

**When**: Customer updates profile
```sql
UPDATE customers SET bio = 'Software engineer' WHERE id = 456;
```

**Then**:
- ✅ Search for "engineer" finds updated profile
- ✅ Cache contains latest bio
- ✅ No stale search results

---

### Use Case 3: Regulatory Audit Trail
**Scenario**: Financial system

**Given**: Orders table
```sql
CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer_id INT,
  amount DECIMAL(15, 2),
  status VARCHAR(50)
);
```

**When**: Order status changes from "pending" to "shipped"
```sql
UPDATE orders SET status = 'shipped' WHERE id = 789;
```

**Then**:
- ✅ Analytics warehouse captures the change
- ✅ Auditors can trace every order modification
- ✅ Compliance: "Who changed it?" "When?" "What was the old value?"

---

## Summary Table

| Requirement | Priority | Measurable? | Challenge Level |
|-------------|----------|-------------|-----------------|
| Change Capture | CRITICAL | Yes (0% data loss) | High (CDC concepts) |
| Kafka Publishing | CRITICAL | Yes (latency <100ms) | Medium (Kafka basics) |
| Redis Sync | HIGH | Yes (TTL consistency) | Medium (consumer patterns) |
| Elasticsearch Sync | HIGH | Yes (search results) | Medium (indexing) |
| Analytics Export | MEDIUM | Yes (event count) | Low (batch processing) |
| Exactly-Once Delivery | CRITICAL | Yes (no duplicates) | High (idempotency) |
| <1 Second Latency | HIGH | Yes (instrumentation) | Medium (tuning) |
| Fault Tolerance | HIGH | Yes (failure testing) | High (chaos engineering) |
| Schema Evolution | MEDIUM | Yes (backward compat) | High (versioning strategy) |

---

## Next Steps

→ Move to [Architecture Overview](../architecture/ARCHITECTURE.md) to see HOW we'll build this
