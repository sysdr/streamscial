#!/bin/bash

source venv/bin/activate

echo "=================================================="
echo "StreamSocial Broker Monitoring - Live Demo"
echo "=================================================="
echo ""

# Check if Kafka is running
if ! docker ps | grep -q day51-broker-1; then
    echo "Starting Kafka cluster..."
    docker-compose up -d
    echo "Waiting 30 seconds for cluster initialization..."
    sleep 30
fi

# Create test topics
echo "Creating test topics..."
docker exec day51-broker-1 kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic user-events \
    --partitions 12 \
    --replication-factor 2 \
    --if-not-exists

docker exec day51-broker-1 kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic content-events \
    --partitions 12 \
    --replication-factor 2 \
    --if-not-exists

docker exec day51-broker-1 kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic engagement-events \
    --partitions 12 \
    --replication-factor 2 \
    --if-not-exists

echo ""
echo "Starting monitoring dashboard..."
echo "Dashboard will be available at: http://localhost:5000"
echo ""
echo "In a separate terminal, run: python src/test_producer.py"
echo "to generate test traffic and see live metrics"
echo ""
echo "Press Ctrl+C to stop"
echo ""

python dashboards/app.py
