#!/bin/bash

echo "🔨 Building StreamSocial Error Handling System..."

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Start infrastructure
echo "🐳 Starting Kafka infrastructure..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Create topics
echo "📝 Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic streamsocial-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic streamsocial-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

echo "✅ Build completed successfully!"
echo ""
echo "🚀 To start the system:"
echo "  ./start.sh"
echo ""
echo "📊 Dashboard will be available at: http://localhost:8000"
echo "🐳 Kafka UI available at: http://localhost:8080 (if using kafka-ui)"
