#!/bin/bash
set -e

echo "🚀 Starting Day 34 SMT Pipeline..."

# Start Docker containers
echo "Starting Kafka infrastructure..."
docker compose up -d

# Wait for services
echo "Waiting for services to be ready..."
sleep 30

# Create topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic raw-user-actions-ios --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic raw-user-actions-android --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic raw-user-actions-web --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic normalized-user-actions --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic dlq-transform-failures --partitions 1 --replication-factor 1 --if-not-exists

echo "✅ Infrastructure started!"
echo "📊 Kafka Connect UI: http://localhost:8083"
echo "🌐 Dashboard will be available at: http://localhost:5000"
