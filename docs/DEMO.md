# Demo Guide (Portfolio Scenarios)

Use this as your main walkthrough script. For full command details and narration, see `docs/DEMO_RUNBOOK.md`.

## Scenarios You Can Demonstrate

1. Happy path CDC flow (INSERT, UPDATE, DELETE).
2. Consumer resilience with retry + DLQ.
3. Replay preflight safety checks.
4. Dashboard-driven operational visibility.
5. Monitoring health checks (Prometheus/Grafana).
6. RBAC behavior (viewer vs admin).

## 0) Pre-Demo Setup

1. Start infra: `docker compose up -d`
2. Start registry platform: `python scripts/start-consumer-registry-platform.py`
3. Start consumers using latest offset for clean output:

```powershell
$env:AUTO_OFFSET_RESET='latest'; mvn -q -f consumers/redis-sync/pom.xml exec:java
```

```powershell
$env:AUTO_OFFSET_RESET='latest'; mvn -q -f consumers/elasticsearch-sync/pom.xml exec:java
```

4. Verify:

```powershell
python scripts/verify-monitoring.py
python scripts/verify-admin-dashboard.py
```

## 1) Scenario: Happy Path CDC

1. Insert a product in PostgreSQL.
2. Show consumer logs processing event.
3. Show dashboard pages: Topics, Consumer Health, Metrics.
4. Update and delete the same product and repeat checks.

## 2) Scenario: Retry + DLQ

1. Explain one malformed/poison message should not stop stream processing.
2. Show consumers log error and continue.
3. Show DLQ visibility in dashboard `DLQ Events` page.

## 3) Scenario: Replay Safety

1. Run replay preflight command.
2. Explain horizon/risk output.
3. Submit replay request from dashboard as admin.
4. Show replay request list and activity timeline.

## 4) Scenario: RBAC

1. Use viewer role for health/metrics read flows.
2. Use admin role for replay submit action.
3. Mention this proves endpoint-level privilege boundaries.

## 5) Scenario: Monitoring

1. Open Prometheus targets page.
2. Open Grafana health.
3. Run `verify-monitoring.py` and show all checks passed.

## LinkedIn Talking Points

1. CDC architecture from WAL to downstream projections.
2. Reliability: shared retry framework + DLQ routing.
3. Operability: replay preflight, dashboard APIs, and monitoring.
4. Practical engineering: deterministic verification scripts.
