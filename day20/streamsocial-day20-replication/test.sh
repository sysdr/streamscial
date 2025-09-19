#!/bin/bash
echo "🧪 Running StreamSocial Day 20 Tests"
echo "===================================="

source venv/bin/activate

# Ensure Docker is running
cd docker
docker-compose up -d
cd ..

echo "⏳ Waiting for Kafka cluster..."
sleep 20

# Run tests
echo "🔬 Running unit tests..."
python -m pytest tests/ -v --tb=short

echo "✅ Tests completed"
