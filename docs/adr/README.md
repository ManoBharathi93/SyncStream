# Architecture Decision Records (ADRs)

ADRs document **significant technical decisions** and the reasoning behind them.

Format: Each ADR follows the RFC 2119 style (MUST, SHOULD, MAY).

## Index of ADRs

| ADR | Title | Status | Phase |
|-----|-------|--------|-------|
| [ADR-001](ADR-001-kafka.md) | Choice of Apache Kafka as Event Bus | ✅ Accepted | 0 |
| [ADR-002](ADR-002-debezium.md) | Choice of Debezium for CDC | ✅ Accepted | 0 |
| [ADR-003](ADR-003-consumer-groups.md) | Consumer Group Strategy for Scalability | ✅ Accepted | 1 |
| [ADR-004](ADR-004-idempotency.md) | Idempotent Consumer Pattern for Exactly-Once | ✅ Accepted | 1 |
| [ADR-005](ADR-005-schema-evolution.md) | Schema Evolution Strategy | 📋 Planned | 2 |
| [ADR-006](ADR-006-monitoring.md) | Observability & Monitoring Strategy | 📋 Planned | 5 |

## How to Read an ADR

Each ADR includes:
1. **Title**: What decision does this document?
2. **Status**: Accepted, Rejected, Pending, Superseded
3. **Context**: Why are we making this decision?
4. **Alternatives Considered**: What else did we evaluate?
5. **Decision**: What we chose and why
6. **Consequences**: Trade-offs and implications
7. **Follow-up Actions**: What needs to happen next?

Example of a well-structured ADR can be found in [ADR-001](ADR-001-kafka.md).

---

## Decision-Making Framework

When proposing a new ADR, ask:

1. **Is this a significant decision?**
   - Affects multiple components? ✅ Record it
   - Can be changed easily later? ❌ Might not need ADR
   - Has long-term implications? ✅ Record it

2. **Have we considered alternatives?**
   - Only one option considered? ❌ Do more research
   - Evaluated 2-3 serious options? ✅ Ready for ADR

3. **Can we explain the trade-offs?**
   - Know what we're giving up? ✅ Document it
   - Know unknown unknowns? ⚠️ Acknowledge risks

---

## How Decisions Are Made

1. **Problem Identified** (team discussion)
2. **Research Phase** (read docs, build POCs)
3. **ADR Drafted** (document decision + alternatives)
4. **Review** (peer review, architecture meeting)
5. **Accepted/Rejected**
6. **Communicated** (team knows the decision)
7. **Reviewed Later** (6 months: is this still the right choice?)

---

## Learn More

→ Read the actual ADRs to understand the reasoning
→ See real ADRs from companies: [Cognitect](https://github.com/cognitect-labs/adr), [AWS](https://docs.aws.amazon.com/prescriptive-guidance/latest/decision-guides/welcome.html)

