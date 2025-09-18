#!/bin/bash

echo "🏗️  Building StreamSocial Async Producer..."

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Start Kafka (Docker)
echo "🐳 Starting Kafka with Docker..."
cd docker
docker-compose up -d
cd ..

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 20

# Create Kafka topics
echo "📝 Creating Kafka topics..."
docker exec -it $(docker ps -q -f name=kafka) kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic content-moderation
docker exec -it $(docker ps -q -f name=kafka) kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic content-moderation-dlq
docker exec -it $(docker ps -q -f name=kafka) kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic timeline-updates

echo "✅ Build complete!"
echo ""
echo "🚀 Next steps:"
echo "  1. Run './start.sh' to start the application"
echo "  2. Open http://localhost:5000/dashboard in your browser"
echo "  3. Test the async producer with the web interface"
echo "  4. Run 'python tests/load_test.py' for load testing"
