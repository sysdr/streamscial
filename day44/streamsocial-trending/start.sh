#!/bin/bash
set -e

echo "=== Starting StreamSocial Trending System ==="

# Check if Kafka is running
if ! nc -z localhost 9092 2>/dev/null; then
    echo "Error: Kafka not running. Start with: docker-compose up -d"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Set PYTHONPATH to project root for absolute imports
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Create Kafka topics
echo "[1/4] Creating Kafka topics..."
docker exec kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic streamsocial.posts \
    --partitions 6 \
    --replication-factor 1 \
    --config retention.ms=86400000 || true

docker exec kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic streamsocial.trending \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=compact || true

# Start dashboard in background
echo "[2/4] Starting dashboard..."
python src/dashboard.py &
DASHBOARD_PID=$!
echo $DASHBOARD_PID > dashboard.pid

# Start post generator
echo "[3/4] Starting post generator..."
python src/utils/post_generator.py &
GENERATOR_PID=$!
echo $GENERATOR_PID > generator.pid

# Start trending processor
echo "[4/4] Starting trending processor..."
python src/trending_processor.py &
PROCESSOR_PID=$!
echo $PROCESSOR_PID > processor.pid

echo ""
echo "✓ System started successfully!"
echo ""
echo "  Dashboard:  http://localhost:5000"
echo "  Logs:       tail -f logs/*.log"
echo ""
echo "To stop: ./stop.sh"

# Wait for user interrupt
wait
