#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5433}"
POSTGRES_USER="${POSTGRES_USER:-admin}"
POSTGRES_DB="${POSTGRES_DB:-streamsocial}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-admin123}"
CONNECT_REST_URL="${CONNECT_REST_URL:-http://localhost:8083}"
CONNECTOR_CONFIG="${ROOT_DIR}/config/debezium/postgres-connector.json"
CONNECTOR_NAME="user-profiles-connector"

echo "=========================================="
echo "Building Day 38: CDC with Debezium"
echo "=========================================="

if [ ! -d "${ROOT_DIR}/venv" ]; then
    echo "Creating virtual environment..."
    python3.11 -m venv "${ROOT_DIR}/venv"
fi

source "${ROOT_DIR}/venv/bin/activate"

echo "Installing dependencies..."
pip install --upgrade pip
pip install -r "${ROOT_DIR}/requirements.txt"

wait_for_postgres() {
    echo "Waiting for PostgreSQL to become ready..."
    local retries=30
    for attempt in $(seq 1 "${retries}"); do
        if docker exec day38-postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc "SELECT 1" >/dev/null 2>&1; then
            echo "✓ PostgreSQL is ready"
            return 0
        fi
        sleep 2
    done
    echo "❌ PostgreSQL did not become ready in time"
    exit 1
}

wait_for_connect() {
    echo "Waiting for Kafka Connect to become ready..."
    local retries=30
    for attempt in $(seq 1 "${retries}"); do
        local status
        status=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_REST_URL}/connectors" || echo "000")
        if [[ "${status}" == 2* || "${status}" == 3* ]]; then
            echo "✓ Kafka Connect is ready"
            return 0
        fi
        sleep 3
    done
    echo "❌ Kafka Connect did not become ready in time"
    exit 1
}

deploy_connector() {
    echo "Deploying Debezium connector (${CONNECTOR_NAME})..."
    local attempts=5
    for attempt in $(seq 1 "${attempts}"); do
        local existing_code
        existing_code=$(curl -s -o /tmp/connector_status.json -w "%{http_code}" \
            "${CONNECT_REST_URL}/connectors/${CONNECTOR_NAME}" || echo "000")

        local deploy_code
        if [ "${existing_code}" = "200" ]; then
            deploy_code=$(curl -s -o /tmp/connector_response.json -w "%{http_code}" \
                -X PUT "${CONNECT_REST_URL}/connectors/${CONNECTOR_NAME}/config" \
                -H "Content-Type: application/json" \
                -d @"${CONNECTOR_CONFIG}" || echo "000")
        else
            deploy_code=$(curl -s -o /tmp/connector_response.json -w "%{http_code}" \
                -X POST "${CONNECT_REST_URL}/connectors" \
                -H "Content-Type: application/json" \
                -d @"${CONNECTOR_CONFIG}" || echo "000")
        fi

        if [[ "${deploy_code}" == 2* ]]; then
            echo "✓ Connector configuration applied"
            return 0
        fi

        echo "Connector deployment failed (HTTP ${deploy_code}). Retrying (${attempt}/${attempts})..."
        sleep 5
    done

    echo "❌ Failed to deploy connector after multiple attempts"
    cat /tmp/connector_response.json || true
    exit 1
}

echo "Starting Docker services..."
export POSTGRES_PORT
cd "${ROOT_DIR}/docker"
docker-compose down -v
docker-compose up -d
cd "${ROOT_DIR}"

wait_for_postgres

echo "Initializing PostgreSQL database..."
docker exec -i day38-postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" < "${ROOT_DIR}/config/postgres/init.sql" || true

wait_for_connect
deploy_connector

echo "✓ Build complete!"
echo ""
echo "Services running:"
echo "  - PostgreSQL: ${POSTGRES_HOST}:${POSTGRES_PORT}"
echo "  - Kafka: localhost:9092"
echo "  - Kafka Connect: ${CONNECT_REST_URL}"
echo "  - Schema Registry: http://localhost:8081"
echo ""
echo "Run ./start.sh to start the demo"
