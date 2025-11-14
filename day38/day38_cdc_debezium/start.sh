#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5433}"
POSTGRES_USER="${POSTGRES_USER:-admin}"
POSTGRES_DB="${POSTGRES_DB:-streamsocial}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-admin123}"
CONNECT_REST_URL="${CONNECT_REST_URL:-http://localhost:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-user-profiles-connector}"
CONNECTOR_CONFIG="${ROOT_DIR}/config/debezium/postgres-connector.json"
DASHBOARD_PORT="${DASHBOARD_PORT:-5001}"

echo "=========================================="
echo "Starting CDC Demo"
echo "=========================================="

source "${ROOT_DIR}/venv/bin/activate"
export POSTGRES_HOST POSTGRES_PORT POSTGRES_USER POSTGRES_DB POSTGRES_PASSWORD DASHBOARD_PORT

deploy_connector() {
    echo "Deploying Debezium connector (${CONNECTOR_NAME})..."
    local response_code
    response_code=$(curl -s -o /tmp/start_connector_response.json -w "%{http_code}" \
        -X POST "${CONNECT_REST_URL}/connectors" \
        -H "Content-Type: application/json" \
        -d @"${CONNECTOR_CONFIG}" || echo "000")
    if [[ "${response_code}" == 2* ]]; then
        echo "✓ Connector deployed"
    else
        echo "❌ Failed to deploy connector (HTTP ${response_code})"
        cat /tmp/start_connector_response.json || true
        exit 1
    fi
}

ensure_connector() {
    echo "Checking connector status..."
    local status_code
    status_code=$(curl -s -o /tmp/start_connector_status.json -w "%{http_code}" \
        "${CONNECT_REST_URL}/connectors/${CONNECTOR_NAME}/status" || echo "000")

    if [[ "${status_code}" == 200 ]]; then
        cat /tmp/start_connector_status.json | python3 -m json.tool
        return 0
    fi

    echo "Connector not deployed yet. Deploying using provided configuration..."
    deploy_connector

    echo "Waiting for connector to start..."
    local retries=10
    for attempt in $(seq 1 "${retries}"); do
        status_code=$(curl -s -o /tmp/start_connector_status.json -w "%{http_code}" \
            "${CONNECT_REST_URL}/connectors/${CONNECTOR_NAME}/status" || echo "000")
        if [[ "${status_code}" == 200 ]]; then
            cat /tmp/start_connector_status.json | python3 -m json.tool
            return 0
        fi
        sleep 3
    done
    echo "❌ Connector did not become healthy in time"
    exit 1
}

ensure_connector

ensure_dashboard_port() {
    if ss -ltn | grep -q ":${DASHBOARD_PORT} "; then
        echo "Port ${DASHBOARD_PORT} is already in use. Set DASHBOARD_PORT to a free port or stop the conflicting service."
        exit 1
    fi

    if pgrep -f "cdc_monitor.py" >/dev/null 2>&1; then
        echo "Stopping existing dashboard process..."
        pkill -f "cdc_monitor.py" || true
        sleep 1
    fi
}

ensure_dashboard_port

echo "Starting monitoring dashboard..."
python "${ROOT_DIR}/dashboards/cdc_monitor.py" &
DASHBOARD_PID=$!
echo "Dashboard PID: ${DASHBOARD_PID}"

sleep 3

echo ""
echo "✓ Services started!"
echo ""
echo "Open dashboard: http://localhost:${DASHBOARD_PORT}"
echo ""
echo "Run demo commands:"
echo "  1. Terminal 1: python src/consumer/cdc_consumer.py"
echo "  2. Terminal 2: python src/producer/profile_updater.py"
echo ""
echo "Press Ctrl+C to stop"

trap "kill ${DASHBOARD_PID} 2>/dev/null" EXIT
wait "${DASHBOARD_PID}"
