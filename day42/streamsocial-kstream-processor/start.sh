#!/bin/bash
set -e

echo "=== Starting StreamSocial KStream Processor ==="

# Start Kafka (if not running)
if ! docker ps | grep -q kafka; then
    echo "Starting Kafka..."
    docker-compose up -d
    echo "Waiting for Kafka to be ready..."
    sleep 15
fi

# Create topics
docker exec $(docker ps -q -f name=kafka) kafka-topics --create --if-not-exists --topic user-interactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec $(docker ps -q -f name=kafka) kafka-topics --create --if-not-exists --topic feed-ranking-signals --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Activate virtual environment and start
source venv/bin/activate
python demo.py
