# SyncStream Demo Runbook (Teach-Me-All Scenarios)

This runbook teaches you how to run and explain SyncStream confidently as a portfolio project.

## A. What to Say in One Line

"SyncStream is an event-driven CDC platform that captures PostgreSQL changes and projects them to Redis and Elasticsearch with retry, DLQ, replay workflows, and operational observability."

## B. Demo Goals

By the end of this run, you will demonstrate:

1. Real-time CDC flow (insert/update/delete).
2. Reliability under bad events (retry + DLQ).
3. Safe replay operations (preflight + request tracking).
4. Operational maturity (dashboard + monitoring).
5. Access boundaries (viewer/admin scenarios).

## C. Prerequisites

1. Docker Desktop is running.
2. Java/Maven available.
3. Python 3.12 available.

Quick checks:

```powershell
docker --version
mvn -version
python --version
```

## D. Terminal Plan

Use five terminals for clean storytelling:

1. Terminal 1: Infra
2. Terminal 2: Registry platform
3. Terminal 3: Redis consumer
4. Terminal 4: Elasticsearch consumer
5. Terminal 5: Verification + SQL operations

## E. Start Sequence (Copy-Paste)

### Terminal 1 - Infrastructure

```powershell
docker compose up -d
```

### Terminal 2 - Registry Platform

```powershell
python scripts/start-consumer-registry-platform.py
```

Expected log:

`Consumer Registry Platform started on port 8091`

### Terminal 3 - Redis Consumer (clean demo mode)

```powershell
$env:AUTO_OFFSET_RESET='latest'
mvn -q -f consumers/retry-framework/pom.xml install
mvn -q -f consumers/redis-sync/pom.xml exec:java
```

### Terminal 4 - Elasticsearch Consumer (clean demo mode)

```powershell
$env:AUTO_OFFSET_RESET='latest'
mvn -q -f consumers/elasticsearch-sync/pom.xml exec:java
```

### Terminal 5 - Verification

```powershell
python scripts/verify-monitoring.py
python scripts/verify-admin-dashboard.py
python scripts/verify-consumer-registration-platform.py
```

Expected signals:

1. `ALL_MONITORING_CHECKS_PASSED`
2. `VERIFY_OK: 19 checks passed`
3. `VERIFY_OK` for registry platform flow

## F. Scenario 1 - Happy Path CDC

### Step 1: Insert

```powershell
docker compose exec postgres psql -U syncstream -d syncstream_db -c "INSERT INTO products(name, description, price, stock_quantity, category, is_active) VALUES ('Demo Product A', 'portfolio insert', 129.00, 30, 'demo', true);"
```

What to show:

1. Redis consumer log shows processing.
2. Elasticsearch consumer log shows processing.
3. Dashboard pages: `Kafka Topics`, `Consumer Health`, `Metrics Dashboard`.

### Step 2: Update

```powershell
docker compose exec postgres psql -U syncstream -d syncstream_db -c "UPDATE products SET price = 99.00, stock_quantity = 25 WHERE name = 'Demo Product A';"
```

### Step 3: Delete

```powershell
docker compose exec postgres psql -U syncstream -d syncstream_db -c "DELETE FROM products WHERE name = 'Demo Product A';"
```

Narration cue:

"This proves end-to-end CDC behavior across create, update, and delete events."

## G. Scenario 2 - Reliability (Retry + DLQ)

Goal: Explain that poison messages do not block the pipeline.

What to do:

1. Keep consumers running.
2. If malformed event appears in logs, point out:
   - processing error was captured,
   - consumer continued processing,
   - event was pushed to DLQ.
3. Open Dashboard `DLQ Events` page to show operational visibility.

Narration cue:

"A single malformed event is isolated through retry policy and DLQ routing; stream processing continues."

## H. Scenario 3 - Replay Safety and Governance

### Step 1: Preflight check

```powershell
python scripts/replay-preflight.py --topic "syncstream.public.products" --consumer-group "syncstream-replay" --pretty-json
```

Explain:

1. Topic and offset horizon are checked first.
2. Replay safety/risk is visible before action.

### Step 2: Replay request in dashboard

1. Open `Replay Requests` page.
2. Submit request with admin role.
3. Show request appears in list/activity timeline.

Narration cue:

"Replay is managed as an operational workflow, not an ad-hoc manual offset hack."

## I. Scenario 4 - Role-Based Access (RBAC)

What to demonstrate:

1. Viewer role can read status/metrics pages.
2. Admin role is required to submit replay.
3. If admin headers are missing, request is rejected.

Narration cue:

"Control-plane APIs enforce role boundaries between observation and mutation paths."

## J. Scenario 5 - Monitoring

Open:

1. Dashboard: http://localhost:8091/dashboard
2. Prometheus: http://localhost:9090
3. Grafana: http://localhost:3001

Show:

1. Prometheus targets are up.
2. Grafana health is good.
3. Monitoring verify script passes.

## K. Troubleshooting During Demo

1. Port 8091 busy:
   - Stop old Java process and restart Terminal 2 command.
2. Old poison messages create noisy startup logs:
   - keep `AUTO_OFFSET_RESET=latest` for demo terminals.
3. Script fails immediately after startup:
   - wait 10 to 20 seconds and rerun.
4. Consumers not reading:
   - verify topic exists and consumers are connected via dashboard/metrics.

## L. 6-Minute Narration Script

### 0:00 to 0:30 - Problem

"Many teams struggle to keep cache and search consistent with a transactional database without brittle dual writes."

### 0:30 to 1:15 - Architecture

"SyncStream captures PostgreSQL WAL changes via Debezium, streams them through Kafka, and projects them to Redis and Elasticsearch with reliability patterns like retry and DLQ."

### 1:15 to 2:15 - Platform up

Run verification scripts and show all pass.

### 2:15 to 4:00 - Live CDC

Run insert, update, delete and show logs/dashboard.

### 4:00 to 5:00 - Reliability

Show DLQ and replay preflight/request flow.

### 5:00 to 6:00 - Engineering quality

"This project includes operational APIs, dashboard workflows, verification scripts, and observability checks, reflecting production-inspired engineering practices."

## M. LinkedIn Caption Starter

"Built SyncStream, a CDC-driven event platform using PostgreSQL, Debezium, Kafka, Redis, and Elasticsearch. Implemented shared retry + DLQ reliability patterns, replay governance, and monitoring with Prometheus/Grafana."

## N. Medium Post Outline

1. Why dual writes fail.
2. CDC architecture and component responsibilities.
3. Reliability patterns (retry, DLQ, replay).
4. Operability and observability design.
5. Lessons learned and production hardening path.
