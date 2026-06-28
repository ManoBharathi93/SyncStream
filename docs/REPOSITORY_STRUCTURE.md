# Repository Structure

This repository is organized to separate infrastructure, platform services, consumer services, and operational verification.

## Top-Level Layout

```
SyncStream/
├── README.md
├── docker-compose.yml
├── consumers/
├── postgres/
├── debezium/
├── monitoring/
├── scripts/
└── docs/
```

## Key Directories

### `consumers/`

- `retry-framework/`: shared retry primitives used by consumer services.
- `redis-sync/`: Kafka consumer projecting CDC events to Redis.
- `elasticsearch-sync/`: Kafka consumer projecting CDC events to Elasticsearch.
- `consumer-registry-platform/`: control-plane service and embedded admin dashboard.

### `postgres/`

- Database schema and seed SQL for local bootstrapping.

### `debezium/`

- Connector configurations defining CDC capture behavior.

### `monitoring/`

- Prometheus and Grafana configuration for local observability.

### `scripts/`

- Operational tooling and deterministic verification scripts.
- Replay safety preflight tooling.

### `docs/`

- Vision, roadmap, architecture, requirements, and ADRs.

## Recommended Reading Order

1. `README.md`
2. `docs/VISION.md`
3. `docs/architecture/ARCHITECTURE.md`
4. `docs/ROADMAP.md`
5. `docs/requirements/REQUIREMENTS.md`
6. `docs/adr/README.md`

## Quality Signal Rules for This Repo

1. Commit code, docs, and verification scripts.
2. Do not commit generated runtime outputs.
3. Keep docs aligned with current implementation status.