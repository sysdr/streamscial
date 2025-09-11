#!/bin/bash

echo "🔨 Building StreamSocial Kafka Batching Project..."

# Create and activate virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Run tests
echo "🧪 Running unit tests..."
python -m pytest tests/unit/ -v

echo "🧪 Running integration tests..."
python -m pytest tests/integration/ -v

# Start Docker services
echo "🐳 Starting Kafka infrastructure..."
cd docker && docker-compose up -d && cd ..

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Create Kafka topics
echo "📝 Creating Kafka topics..."
docker exec -it $(docker ps -q -f "name=kafka") kafka-topics --create --topic streamsocial.posts --bootstrap-server localhost:9092 --partitions 12 --replication-factor 1
docker exec -it $(docker ps -q -f "name=kafka") kafka-topics --create --topic streamsocial.metrics --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1

echo "✅ Build completed successfully!"
echo "🌐 Access dashboard at: http://localhost:5000"
echo "📊 Access Prometheus at: http://localhost:9090"
echo "📈 Access Grafana at: http://localhost:3000 (admin/admin)"

