#!/bin/bash
set -e

echo "========================================="
echo "Building Day 50: Processor API System"
echo "========================================="

# Activate virtual environment
source venv/bin/activate

# Run tests
echo -e "\n1. Running unit tests..."
python -m pytest tests/ -v

# Start Docker containers
echo -e "\n2. Starting Docker containers..."
docker-compose up -d

# Wait for services
echo -e "\n3. Waiting for services to be ready..."
sleep 15

# Check Kafka
echo "Checking Kafka..."
docker exec day50-kafka kafka-topics --bootstrap-server localhost:9092 --list || echo "Kafka not ready yet, continuing..."

# Start dashboard server
echo -e "\n4. Starting dashboard server..."
cd dashboards && python3 -m http.server 8000 &
DASHBOARD_PID=$!
cd ..

echo -e "\n✅ Build complete!"
echo -e "\nServices:"
echo "  - Kafka: localhost:9092"
echo "  - Redis: localhost:6379"
echo "  - API: http://localhost:8080"
echo "  - Dashboard: http://localhost:8000"
echo -e "\nRun './start.sh' to start the processor topology"
