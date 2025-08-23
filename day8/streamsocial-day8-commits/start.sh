#!/bin/bash

echo "🚀 Starting StreamSocial Day 8: Commit Strategies & Reliability Demo"

# Activate virtual environment
source kafka-mastery-env/bin/activate

# Start Docker services
echo "📦 Starting Kafka, PostgreSQL, and Redis..."
cd docker
docker-compose up -d
cd ..

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 15

# Create Kafka topic
echo "📋 Creating Kafka topic..."
docker exec -it $(docker ps -q --filter "ancestor=confluentinc/cp-kafka:7.5.0") kafka-topics --create --topic user-engagements --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Start the application
echo "🎯 Starting StreamSocial Engagement Pipeline..."
echo "Dashboard will be available at: http://localhost:8000"
echo "Press Ctrl+C to stop the demo"

python -m src.main --mode demo

echo "✅ Demo completed successfully!"
