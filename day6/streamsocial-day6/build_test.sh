#!/bin/bash

echo "🔨 Building and testing StreamSocial Consumer Groups"

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run syntax checks
echo "📝 Checking Python syntax..."
python -m py_compile src/consumers/feed_consumer.py
python -m py_compile src/producers/activity_producer.py
python -m py_compile src/utils/consumer_manager.py
python -m py_compile src/monitoring/dashboard.py

# Start minimal infrastructure for testing
cd docker && docker-compose up -d zookeeper kafka redis
cd ..

sleep 20

# Create test topics
docker exec broker kafka-topics --create --topic user-activities --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic generated-feeds --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic consumer-metrics --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

# Run tests
echo "🧪 Running unit tests..."
pytest tests/ -v

# Quick functional test
echo "🔍 Running functional test..."
timeout 30 python -m src.producers.activity_producer 10 5 &
sleep 2
timeout 30 python -m src.consumers.feed_consumer test-consumer &
sleep 10

echo "✅ Build and test complete"

# Cleanup
cd docker && docker-compose down
cd ..
