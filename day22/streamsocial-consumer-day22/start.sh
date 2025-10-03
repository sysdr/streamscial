#!/bin/bash

set -e

echo "🚀 Starting StreamSocial Low-Latency Consumer"

# Start with Docker Compose
cd docker
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 10

# Create topics
echo "📝 Creating Kafka topics..."
docker-compose exec kafka kafka-topics --create --topic critical-notifications --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
docker-compose exec kafka kafka-topics --create --topic important-notifications --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true
docker-compose exec kafka kafka-topics --create --topic standard-notifications --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true

echo "✅ Services started!"
echo ""
echo "🌐 Dashboard: http://localhost:5000"
echo "📊 Metrics: http://localhost:8080/metrics" 
echo "🔍 Logs: docker-compose logs -f consumer"
echo ""
echo "Run './scripts/demo.sh' for a demonstration"
