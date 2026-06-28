# SyncStream

SyncStream is a production-inspired CDC platform that captures PostgreSQL row changes and projects them to downstream systems through Kafka.

## Why This Project Matters

- Demonstrates event-driven design with clear source-of-truth boundaries.
- Shows reliability patterns recruiters expect: retries, DLQ, replay checks, health verification, and observability.
- Includes an admin dashboard and management APIs for operational workflows.

## Architecture at a Glance
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/39e5d14a-e8be-4f9b-b1ed-07b38190c622" />

1. PostgreSQL emits WAL changes.
2. Debezium captures changes and writes Kafka topics (`syncstream.public.*`).
3. Consumers project to Redis and Elasticsearch.
4. Failures flow through retry policy and dead-letter topics.
5. Consumer Registry Platform provides registration, governance, replay requests, and dashboard APIs.

## Implemented Components

- `consumers/retry-framework`: shared retry primitives (`RetryPolicy`, backoff strategy, failure classifier).
- `consumers/redis-sync`: Kafka to Redis projector with retry + DLQ.
- `consumers/elasticsearch-sync`: Kafka to Elasticsearch projector with retry + DLQ.
- `consumers/consumer-registry-platform`: control-plane service + admin dashboard + replay management APIs.
- `monitoring/`: Prometheus and Grafana config.
- `scripts/verify-*.py`: deterministic verification scripts for key workflows.

## Quick Start

1. Start infrastructure.

```powershell
docker compose up -d
```

2. Start services (in separate terminals).

```powershell
python scripts/start-consumer-registry-platform.py
```

```powershell
mvn -f consumers/retry-framework/pom.xml install
mvn -f consumers/redis-sync/pom.xml exec:java
```

```powershell
mvn -f consumers/elasticsearch-sync/pom.xml exec:java
```

3. Run verification checks.

```powershell
python scripts/verify-monitoring.py
python scripts/verify-admin-dashboard.py
```

## Operations Endpoints

- Admin dashboard: `http://localhost:8091/dashboard`
- Registry APIs base: `http://localhost:8091/api/v1`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`

## Current Scope Notes

- End-to-end flow is implemented and verifiable for product events.
- Analytics consumer is not yet implemented.
- Security is role-header based for local/dev workflows and should be replaced with signed identity (JWT/OIDC) for production.

## Documentation

- `docs/VISION.md`: business problem and engineering vision.
- `docs/ROADMAP.md`: what is done and what remains.
- `docs/REPOSITORY_STRUCTURE.md`: curated repo map.
- `docs/architecture/ARCHITECTURE.md`: system architecture and data flow.
- `docs/requirements/REQUIREMENTS.md`: functional and non-functional requirements.

  ## Experiment Results:
  ### 1. Change Data Capture:
  <img width="942" height="823" alt="image" src="https://github.com/user-attachments/assets/45fc97fe-6ea5-491a-b53f-6e425bf24c08" />

  ### 2. Dual write Path:
  <img width="920" height="742" alt="image" src="https://github.com/user-attachments/assets/c920a68c-a6be-4b59-be5a-7d9d252572b9" />

  ### 3. Polling:
  <img width="937" height="828" alt="image" src="https://github.com/user-attachments/assets/e9ce074c-2c45-44b9-8112-5c7b6d226d91" />


# TradeOff radar:
<img width="898" height="723" alt="image" src="https://github.com/user-attachments/assets/794bb4e6-bfad-4e48-a8b5-4eaf6c74e108" />

  
