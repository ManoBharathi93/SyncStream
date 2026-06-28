# SyncStream Roadmap

## Status Snapshot

### Completed

1. CDC infrastructure stack (PostgreSQL + Debezium + Kafka + Redis + Elasticsearch).
2. Redis projection consumer with retry and DLQ handling.
3. Elasticsearch projection consumer with retry and DLQ handling.
4. Consumer Registry Platform with management APIs.
5. Admin dashboard with operational pages (CDC status, connector status, topics, health, DLQ, replay, consumer registry, metrics).
6. Monitoring stack with Prometheus + Grafana + exporter checks.
7. Verification scripts for dashboard and monitoring.

### In Progress / Next

1. Analytics consumer module.
2. Strong authn/authz boundary (JWT/OIDC) for management APIs.
3. Automated CI workflow for module builds, tests, and verification scripts.
4. One-command service orchestration for app services in addition to infra.

## Milestone Plan

### Milestone A: Showcase Baseline (Done)

- Working event-driven sync flow.
- Demonstrable reliability patterns (retry + DLQ + replay preflight + health checks).
- Operable admin interface for platform workflows.

### Milestone B: Production-Ready Control Plane

- Replace header-trust role model with signed identity.
- Add audit-safe auth context propagation.
- Add replay approvals and stronger policy checks.

### Milestone C: Full Platform Completeness

- Implement analytics sink consumer.
- Expand table/topic coverage beyond products-first path.
- Add richer SLO dashboards and runbooks.

## Definition of Done for Public Showcase

1. All module tests passing.
2. Dashboard verification script passing.
3. Monitoring verification script passing.
4. Readme and docs aligned with current system behavior.
5. No generated artifacts or local runtime noise committed to repo.