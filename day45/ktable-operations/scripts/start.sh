#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "Starting Kafka infrastructure..."
cd "$PROJECT_DIR/docker" && docker-compose up -d && cd "$PROJECT_DIR"

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Creating topics..."
KAFKA_CONTAINER=$(docker ps -q -f name=kafka)
if [ -z "$KAFKA_CONTAINER" ]; then
    echo "Error: Kafka container not found"
    exit 1
fi

docker exec -i "$KAFKA_CONTAINER" kafka-topics \
    --create --if-not-exists \
    --topic user-actions \
    --bootstrap-server localhost:9092 \
    --partitions 6 \
    --replication-factor 1 || echo "Topic user-actions may already exist"

docker exec -i "$KAFKA_CONTAINER" kafka-topics \
    --create --if-not-exists \
    --topic reputation-changelog \
    --bootstrap-server localhost:9092 \
    --partitions 6 \
    --replication-factor 1 \
    --config cleanup.policy=compact || echo "Topic reputation-changelog may already exist"

if [ ! -f "$PROJECT_DIR/venv/bin/activate" ]; then
    echo "Error: Virtual environment not found. Run ./scripts/build.sh first"
    exit 1
fi

source "$PROJECT_DIR/venv/bin/activate"

# Install/update dependencies
echo "Installing dependencies..."
pip install -q -r "$PROJECT_DIR/requirements.txt"

# Set PYTHONPATH so Python can find src and config modules
export PYTHONPATH="$PROJECT_DIR:$PYTHONPATH"

echo "Starting Query API..."
cd "$PROJECT_DIR"
PYTHONPATH="$PROJECT_DIR" python "$PROJECT_DIR/src/query_api.py" > "$PROJECT_DIR/query_api.log" 2>&1 &
QUERY_PID=$!

echo "Starting Dashboard..."
PYTHONPATH="$PROJECT_DIR" python "$PROJECT_DIR/dashboard/app.py" > "$PROJECT_DIR/dashboard.log" 2>&1 &
DASHBOARD_PID=$!

echo "Starting Streams Processor..."
PYTHONPATH="$PROJECT_DIR" python "$PROJECT_DIR/src/streams_processor.py" > "$PROJECT_DIR/processor.log" 2>&1 &
PROCESSOR_PID=$!

sleep 3

echo "Starting Event Producer..."
PYTHONPATH="$PROJECT_DIR" python "$PROJECT_DIR/src/event_producer.py" > "$PROJECT_DIR/producer.log" 2>&1 &
PRODUCER_PID=$!

echo ""
echo "========================================="
echo "All services started!"
echo "Dashboard: http://localhost:8002"
echo "Query API: http://localhost:8001/docs"
echo "========================================="
echo ""
echo "Process IDs saved to .pids"
echo "$QUERY_PID $DASHBOARD_PID $PROCESSOR_PID $PRODUCER_PID" > "$PROJECT_DIR/.pids"
echo "Logs:"
echo "  Query API: $PROJECT_DIR/query_api.log"
echo "  Dashboard: $PROJECT_DIR/dashboard.log"
echo "  Processor: $PROJECT_DIR/processor.log"
echo "  Producer: $PROJECT_DIR/producer.log"

