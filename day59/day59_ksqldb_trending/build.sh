#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "=== Building Day 59: ksqlDB Trending Analytics ==="

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "ERROR: Virtual environment not found. Please run setup.sh first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Start Docker infrastructure
echo "Starting Docker containers..."
cd docker
docker-compose up -d
cd ..

# Wait for services
echo "Waiting for services to be ready..."
sleep 30

# Verify Kafka
echo "Verifying Kafka connectivity..."
timeout 60 bash -c 'until docker exec day59-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; do sleep 2; done'

# Verify ksqlDB
echo "Waiting for ksqlDB server..."
timeout 90 bash -c 'until curl -sf http://localhost:8088/info &>/dev/null; do sleep 3; done'

echo "✓ All services are ready!"

# Setup ksqlDB streams and tables
echo "Setting up ksqlDB streams and tables..."
python src/ksqldb_manager.py

echo "✓ Build complete!"
echo ""
echo "Next steps:"
echo "  ./start.sh    - Start event generation and dashboard"
echo "  ./test.sh     - Run test suite"
echo "  ./demo.sh     - Run full demonstration"
echo "  ./stop.sh     - Stop all services"
