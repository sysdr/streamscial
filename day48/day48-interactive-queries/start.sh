#!/bin/bash

echo "======================================="
echo "Starting Interactive Queries System"
echo "======================================="

# Start Docker services
echo "Starting Kafka infrastructure..."
docker-compose up -d

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
sleep 15

# Activate virtual environment
source venv/bin/activate

# Create logs directory
mkdir -p logs

# Start Kafka Streams instance 1
echo "Starting Streams instance 1..."
python src/main.py 0 > logs/instance1.log 2>&1 &
INSTANCE1_PID=$!

sleep 5

echo ""
echo "=========================================="
echo "System Started!"
echo "=========================================="
echo "Instance 1: http://localhost:8080"
echo "Dashboard: http://localhost:8080/web/dashboard.html"
echo ""
echo "Open dashboard in browser and run:"
echo "  python src/post_generator.py"
echo ""
echo "Or use './demo.sh' to run automated demo"
echo "=========================================="

# Store PIDs
echo $INSTANCE1_PID > .pids
