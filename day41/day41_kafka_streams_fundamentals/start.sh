#!/bin/bash

echo "Starting Kafka Streams Engagement Scorer..."

# Get absolute path to current directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR"

# Activate virtual environment
if [ ! -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    echo "Error: Virtual environment not found. Please run setup.sh first."
    exit 1
fi
source "$SCRIPT_DIR/venv/bin/activate"

# Start Docker Compose
echo "Starting Kafka..."
cd "$SCRIPT_DIR"
docker-compose up -d

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
sleep 10

# Create topics
KAFKA_CONTAINER=$(docker ps -qf "name=kafka")
if [ -z "$KAFKA_CONTAINER" ]; then
    echo "Error: Kafka container not found"
    exit 1
fi

docker exec $KAFKA_CONTAINER kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic user-interactions \
    --partitions 6 \
    --replication-factor 1

docker exec $KAFKA_CONTAINER kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic engagement-scores \
    --partitions 6 \
    --replication-factor 1

docker exec $KAFKA_CONTAINER kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic engagement-scores-changelog \
    --partitions 6 \
    --replication-factor 1 \
    --config cleanup.policy=compact

echo "Kafka topics created!"

# Get absolute path to current directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR"

# Start event producer in background
echo "Starting event producer..."
python "$SCRIPT_DIR/src/event_producer.py" &
PRODUCER_PID=$!

# Wait a bit for events to start flowing
sleep 5

# Start query service with integrated topology
echo "Starting query service with stream processor..."
python -c "
import sys
import os
sys.path.insert(0, '$SCRIPT_DIR')

from src.query_service import start_query_service
from src.engagement_scorer import EngagementScoringTopology
import threading

topology = EngagementScoringTopology(
    bootstrap_servers='localhost:9092',
    group_id='engagement-scorer-v1',
    state_dir='/tmp/kafka-streams-state'
)

# Start topology in background thread
processor_thread = threading.Thread(target=topology.start, daemon=True)
processor_thread.start()

# Wait for topology to initialize
import time
time.sleep(3)

# Start query service (this will block)
start_query_service(topology, host='0.0.0.0', port=5001)
" &
QUERY_PID=$!

echo ""
echo "==================================="
echo "Services Started!"
echo "==================================="
echo "Dashboard: http://localhost:5001/engagement_dashboard.html"
echo "API: http://localhost:5001"
echo "Query top scores: curl http://localhost:5001/scores/top"
echo ""
echo "Process IDs:"
echo "Producer: $PRODUCER_PID"
echo "Processor: $PROCESSOR_PID"
echo "Query Service: $QUERY_PID"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for interrupt
wait
