#!/bin/bash
set -e

echo "Starting StreamSocial ML Training Data Sync..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source venv/bin/activate

# Start Docker services
echo "Starting Docker services..."
cd docker
docker compose up -d || docker-compose up -d
cd ..

# Wait for services
echo "Waiting for services to be ready..."
sleep 20

# Initialize database
echo "Initializing database and generating test data..."
python -c "from src.models import init_database; init_database()"
python -c "from src.data_generator import main; main()"

# Start monitoring API
echo "Starting monitoring API on port 8080..."
python -m uvicorn src.api.monitoring_api:app --host 0.0.0.0 --port 8080 &

echo ""
echo "=============================================="
echo "StreamSocial ML Training Data Sync Started!"
echo "=============================================="
echo ""
echo "Dashboard: http://localhost:8080"
echo "Kafka Connect: http://localhost:8083"
echo "Schema Registry: http://localhost:8081"
echo ""
