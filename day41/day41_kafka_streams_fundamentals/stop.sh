#!/bin/bash

echo "Stopping Kafka Streams services..."

# Get absolute path to current directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR"

# Kill Python processes
pkill -f "event_producer.py" || true
pkill -f "engagement_scorer.py" || true
pkill -f "query_service.py" || true

# Stop Docker Compose
cd "$SCRIPT_DIR"
docker-compose down

echo "All services stopped!"
