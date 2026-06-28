#!/usr/bin/env bash
# ============================================================
# register-connector.sh
# Registers the Debezium PostgreSQL connector via REST API.
#
# Run this AFTER `docker compose up -d` and after Debezium
# is healthy (check with: docker compose ps).
#
# Usage:
#   ./scripts/register-connector.sh
#
# Prerequisites:
#   - All containers are running (docker compose up -d)
#   - Debezium REST API is available on localhost:8083
#   - curl is installed on your host
# ============================================================

set -euo pipefail

DEBEZIUM_URL="http://localhost:8083"
CONNECTOR_CONFIG="./debezium/connector-config-clean.json"
CONNECTOR_NAME="postgres-cdc-connector"

echo ""
echo "=== SyncStream: Registering Debezium Connector ==="
echo ""

# ── Step 1: Wait for Debezium to be ready ─────────────────
echo "[1/4] Waiting for Debezium REST API to be available..."

MAX_RETRIES=20
RETRY_COUNT=0
until curl -sf "${DEBEZIUM_URL}/connectors" > /dev/null 2>&1; do
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
    echo "ERROR: Debezium REST API not available after ${MAX_RETRIES} attempts."
    echo "Check logs: docker compose logs kafka-connect"
    exit 1
  fi
  echo "  Waiting... (attempt ${RETRY_COUNT}/${MAX_RETRIES})"
  sleep 5
done
echo "  Debezium is ready."

# ── Step 2: Check if connector already exists ─────────────
echo ""
echo "[2/4] Checking if connector already registered..."

EXISTING=$(curl -sf "${DEBEZIUM_URL}/connectors" 2>/dev/null || echo "[]")
if echo "$EXISTING" | grep -q "$CONNECTOR_NAME"; then
  echo "  Connector '${CONNECTOR_NAME}' already exists."
  echo "  To re-register, delete first:"
  echo "  curl -X DELETE ${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}"
  exit 0
fi
echo "  Connector not yet registered. Proceeding."

# ── Step 3: Register the connector ────────────────────────
echo ""
echo "[3/4] Registering connector from: ${CONNECTOR_CONFIG}"

HTTP_STATUS=$(curl -s -o /tmp/debezium_response.json -w "%{http_code}" \
  -X POST "${DEBEZIUM_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${CONNECTOR_CONFIG}")

if [ "$HTTP_STATUS" -eq 201 ]; then
  echo "  Connector registered successfully (HTTP 201)."
elif [ "$HTTP_STATUS" -eq 409 ]; then
  echo "  Connector already exists (HTTP 409). No action needed."
else
  echo "  ERROR: Unexpected HTTP status: ${HTTP_STATUS}"
  echo "  Response:"
  cat /tmp/debezium_response.json
  exit 1
fi

# ── Step 4: Verify connector status ───────────────────────
echo ""
echo "[4/4] Verifying connector status..."
sleep 5

STATUS_JSON=$(curl -sf "${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || echo "{}")
CONNECTOR_STATE=$(echo "$STATUS_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")

echo ""
echo "  Connector state: ${CONNECTOR_STATE}"
echo ""

if [ "$CONNECTOR_STATE" = "RUNNING" ]; then
  echo "✅ Connector is RUNNING. CDC pipeline is active."
  echo ""
  echo "Next steps:"
  echo "  1. Open Kafka UI:         http://localhost:8080"
  echo "  2. Look for topics:       syncstream.public.products"
  echo "                            syncstream.public.customers"
  echo "                            syncstream.public.orders"
  echo "  3. The initial snapshot events should already be arriving."
  echo "  4. Try inserting a row:   psql -h localhost -U syncstream -d syncstream_db"
  echo "     SQL: INSERT INTO products (name, price, stock_quantity) VALUES ('Test', 9.99, 1);"
else
  echo "⚠️  Connector state is: ${CONNECTOR_STATE}"
  echo ""
  echo "Troubleshooting:"
  echo "  Check connector logs:"
  echo "    docker compose logs kafka-connect | grep -i error"
  echo "  Check connector tasks:"
  echo "    curl -s ${DEBEZIUM_URL}/connectors/${CONNECTOR_NAME}/tasks/0/status | python3 -m json.tool"
  echo "  Common causes:"
  echo "    - PostgreSQL not yet ready"
  echo "    - Wrong credentials in connector-config.json"
  echo "    - wal_level not set to 'logical' in PostgreSQL"
fi

echo ""
echo "=== Done ==="
