#!/bin/bash

echo "🚀 Starting StreamSocial Idempotent Producer System..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3.11 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies if needed
if [ ! -f "venv/pyvenv.cfg" ] || ! pip list | grep -q kafka-python; then
    echo "📥 Installing dependencies..."
    pip install -r requirements.txt
fi

# Start Kafka using Docker
echo "🐳 Starting Kafka infrastructure..."
cd docker
docker-compose up -d
cd ..

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 15

# Create topics
echo "📝 Creating Kafka topics..."
docker exec -it docker_kafka_1 kafka-topics --create --topic streamsocial-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || true
docker exec -it docker_kafka_1 kafka-topics --create --topic streamsocial-metrics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || true

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Start the dashboard
echo "🌐 Starting StreamSocial Dashboard..."
echo "Dashboard will be available at: http://localhost:5000"
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python src/dashboard.py

echo "✅ StreamSocial system started successfully!"
