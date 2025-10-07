#!/bin/bash
set -e

echo "🚀 Starting StreamSocial Headers System..."

# Start Docker services
echo "🐳 Starting Kafka and Redis..."
docker-compose -f docker/docker-compose.yml up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 10

# Create topics
docker-compose -f docker/docker-compose.yml exec kafka kafka-topics --create --topic posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists || true

echo "✅ All services started!"
echo "💡 Services available:"
echo "   - Kafka: localhost:9092"
echo "   - Redis: localhost:6379"
echo "   - Dashboard: python src/web/dashboard.py (then http://localhost:8000)"
