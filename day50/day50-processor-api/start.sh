#!/bin/bash
set -e

echo "========================================="
echo "Starting Processor API System"
echo "========================================="

# Activate virtual environment
source venv/bin/activate

# Create topics
echo "Creating Kafka topics..."
docker exec day50-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic user-engagement \
    --partitions 3 \
    --replication-factor 1

docker exec day50-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic content-metadata \
    --partitions 3 \
    --replication-factor 1

docker exec day50-kafka kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic user-preferences \
    --partitions 3 \
    --replication-factor 1

echo -e "\n✅ Starting application..."
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python src/main.py
