#!/bin/bash
set -e

echo "=========================================="
echo "Starting StreamSocial Day 40 Services"
echo "=========================================="

# Check if Docker is available
if command -v docker-compose &> /dev/null; then
    echo "Starting Docker services..."
    cd docker
    docker-compose up -d
    cd ..
    
    echo "Waiting for Kafka to be ready..."
    sleep 15
else
    echo "Docker not available - ensure Kafka and Redis are running locally"
fi

# Activate virtual environment
source venv/bin/activate

# Create Kafka topics
echo "Creating Kafka topics..."
kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 \
    --topic user-posts --partitions 3 --replication-factor 1 2>/dev/null || true

kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 \
    --topic moderation-dlq --partitions 3 --replication-factor 1 2>/dev/null || true

echo "✓ Topics created"

# Start services in background
echo "Starting monitoring dashboard..."
python src/monitoring_dashboard.py &
DASHBOARD_PID=$!

sleep 3

echo "Starting DLQ retry service..."
python src/dlq_retry_service.py &
DLQ_PID=$!

sleep 2

echo "Starting moderation connector..."
python src/moderation_connector.py &
CONNECTOR_PID=$!

sleep 2

echo "Producing test data..."
python src/test_data_producer.py

echo ""
echo "=========================================="
echo "Services Running"
echo "=========================================="
echo "Dashboard: http://localhost:5000"
echo "Moderation Connector PID: $CONNECTOR_PID"
echo "DLQ Retry Service PID: $DLQ_PID"
echo "Dashboard PID: $DASHBOARD_PID"
echo ""
echo "Press Ctrl+C to stop all services"
echo "=========================================="

# Save PIDs
echo "$CONNECTOR_PID" > /tmp/connector.pid
echo "$DLQ_PID" > /tmp/dlq.pid
echo "$DASHBOARD_PID" > /tmp/dashboard.pid

# Wait
wait
