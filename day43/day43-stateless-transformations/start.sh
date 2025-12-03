#!/bin/bash

set -e

echo "Starting StreamSocial Content Moderation Pipeline..."

# Start Docker services
echo "Starting Kafka cluster..."
cd docker
docker-compose up -d
cd ..

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Create topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic social.posts.raw \
  --partitions 6 \
  --replication-factor 1

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic social.posts.moderated \
  --partitions 6 \
  --replication-factor 1

docker exec kafka kafka-topics --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic social.notifications \
  --partitions 6 \
  --replication-factor 1

# Activate venv
source venv/bin/activate

# Start processor in background
echo "Starting stream processor..."
python src/stream_processor.py &
PROCESSOR_PID=$!

# Start dashboard in background
echo "Starting dashboard..."
python web/dashboard.py &
DASHBOARD_PID=$!

echo ""
echo "✓ Pipeline started successfully!"
echo ""
echo "Stream Processor PID: $PROCESSOR_PID"
echo "Dashboard PID: $DASHBOARD_PID"
echo ""
echo "Access points:"
echo "- Dashboard: http://localhost:5050"
echo "- Prometheus: http://localhost:9090"
echo "- Metrics: http://localhost:8000/metrics"
echo ""
echo "To stop: ./stop.sh"
