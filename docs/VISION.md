# SyncStream Vision

## Problem

Modern systems keep transactional truth in PostgreSQL, but product features also depend on caches, search indices, and operational analytics. Keeping those systems consistent with ad-hoc dual writes creates drift and hidden failure modes.

## Vision Statement

SyncStream provides a reliable CDC backbone so every committed database change can be propagated to downstream systems through an event-driven architecture with operational controls.

## Product Outcomes

1. Near real-time propagation of database changes.
2. Reliable consumer-side projection with retries and dead-letter routing.
3. Operational governance via consumer registry, replay workflows, and dashboard visibility.
4. Interview-grade demonstration of practical microservice and event-driven engineering.

## Implemented Today

1. Debezium pipeline from PostgreSQL WAL to Kafka topics.
2. Redis and Elasticsearch consumers for product-event projections.
3. Shared retry framework used by consumers.
4. Dead-letter publishing paths on terminal projection failure.
5. Consumer Registry Platform with management APIs.
6. Admin dashboard served by the platform for operational tasks.
7. Monitoring stack and verification scripts.

## Scope Boundaries

1. Local-first deployment and validation (Docker Compose + local processes).
2. Product-topic flow is the primary validated path.
3. Analytics consumer remains a planned extension.
4. Current role model is header-based and intentionally marked for production hardening.

## Engineering Principles

1. Source of truth remains in PostgreSQL.
2. Event transport is decoupled from projection storage.
3. Failure is expected and handled through bounded retries and DLQ.
4. Operational workflows should be auditable and script-verifiable.

## Success Criteria

1. End-to-end change propagation works reliably for supported topics.
2. Failed projection events are retried and eventually routed to DLQ with context.
3. Replay operations can be preflighted and requested safely.
4. Dashboard and monitoring expose enough signal for diagnosis.