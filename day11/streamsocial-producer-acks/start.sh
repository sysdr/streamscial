#!/bin/bash

echo "🚀 Starting StreamSocial Producer Acknowledgment Demo"
echo "=================================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run the setup script first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Start Kafka with Docker Compose
echo "🐳 Starting Kafka cluster..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 15

# Create topics
echo "📝 Creating Kafka topics..."
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic streamsocial-critical-events --partitions 3 --replication-factor 1 || true
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic streamsocial-social-events --partitions 3 --replication-factor 1 || true
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic streamsocial-analytics-events --partitions 3 --replication-factor 1 || true

echo "✅ Kafka topics created successfully"

# Run tests
echo "🧪 Running tests..."
python -m pytest src/tests/ -v

# Run benchmark
echo "📊 Running performance benchmark..."
python src/tests/benchmark.py

# Start the main application
echo "🌐 Starting StreamSocial Dashboard..."
echo "Dashboard will be available at: http://localhost:5000"
python src/main.py
