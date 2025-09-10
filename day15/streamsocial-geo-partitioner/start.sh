#!/bin/bash
set -e

echo "🚀 Starting StreamSocial Geo-Partitioner services..."

# Start infrastructure
cd docker
docker-compose up -d
cd ..

# Wait for services
echo "⏳ Waiting for Kafka to start..."
sleep 30

# Create topic with 12 partitions
kafka-topics --create --topic streamsocial-posts --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 || true

# Activate Python environment
source venv/bin/activate

# Start monitoring in background
echo "📊 Starting monitoring dashboard..."
python src/main/python/geo_monitor.py &
MONITOR_PID=$!

# Start producer simulation
echo "📤 Starting producer simulation..."
python src/main/python/geo_producer.py &
PRODUCER_PID=$!

echo "✅ All services started!"
echo "📊 Monitoring Dashboard: http://localhost:5000"
echo "🔍 Kafka Topics:"
kafka-topics --list --bootstrap-server localhost:9092

echo "💡 Services running in background. Use './stop.sh' to stop all services."
echo $MONITOR_PID > monitor.pid
echo $PRODUCER_PID > producer.pid
