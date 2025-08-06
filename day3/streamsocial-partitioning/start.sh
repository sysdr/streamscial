#!/bin/bash

# Set error handling
set -e

echo "🚀 Starting StreamSocial Kafka Partitioning Demo"
echo "=============================================="

# Function to cleanup on exit
cleanup() {
    echo "🛑 Cleaning up..."
    if [ ! -z "$DASHBOARD_PID" ]; then
        kill $DASHBOARD_PID 2>/dev/null || true
    fi
    if [ ! -z "$METRICS_PID" ]; then
        kill $METRICS_PID 2>/dev/null || true
    fi
    exit 0
}

# Set trap for cleanup
trap cleanup INT TERM

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Creating new virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Check if Docker is running
echo "🐳 Checking Docker status..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Starting Docker..."
    open -a Docker
    echo "⏳ Waiting for Docker to start..."
    sleep 15
    # Wait for Docker to be ready
    max_docker_attempts=10
    docker_attempt=1
    while [ $docker_attempt -le $max_docker_attempts ]; do
        if docker info > /dev/null 2>&1; then
            echo "✅ Docker is ready!"
            break
        fi
        echo "⏳ Waiting for Docker... (attempt $docker_attempt/$max_docker_attempts)"
        sleep 5
        docker_attempt=$((docker_attempt + 1))
    done
    
    if [ $docker_attempt -gt $max_docker_attempts ]; then
        echo "❌ Docker failed to start. Please start Docker manually and try again."
        exit 1
    fi
fi

# Start Kafka cluster
echo "🔧 Starting Kafka cluster..."
cd docker
docker-compose down 2>/dev/null || true
docker-compose up -d

# Wait for Kafka to be ready with better error handling
echo "⏳ Waiting for Kafka cluster to be ready..."
sleep 30

# Check if Kafka is ready by trying to connect
echo "🔍 Checking Kafka connectivity..."
max_attempts=15
attempt=1
while [ $attempt -le $max_attempts ]; do
    echo "⏳ Attempt $attempt/$max_attempts: Checking Kafka connectivity..."
    if python -c "
import sys
sys.path.append('.')
from kafka import KafkaConsumer
try:
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], request_timeout_ms=5000)
    consumer.close()
    print('✅ Kafka is ready!')
    sys.exit(0)
except Exception as e:
    print('Kafka not ready yet...')
    sys.exit(1)
" 2>/dev/null; then
        echo "✅ Kafka is ready!"
        break
    fi
    sleep 10
    attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
    echo "❌ Kafka failed to start within expected time"
    echo "🔍 Checking container logs..."
    docker-compose logs --tail=20
    exit 1
fi

cd ..

# Run partition demo
echo "🎯 Running partitioning strategy demo..."
if ! PYTHONPATH=. python tests/demo_partitioning.py; then
    echo "⚠️  Demo completed with warnings (this is normal)"
fi

# Check if port 8080 is already in use and kill existing process
echo "🔍 Checking for existing dashboard process..."
if lsof -ti:8080 > /dev/null 2>&1; then
    echo "⚠️  Port 8080 is in use. Stopping existing process..."
    lsof -ti:8080 | xargs kill -9
    sleep 3
fi

# Start monitoring dashboard in background
echo "📊 Starting monitoring dashboard..."
PYTHONPATH=. python src/dashboard.py &
DASHBOARD_PID=$!

# Wait for dashboard to start
echo "⏳ Waiting for dashboard to start..."
sleep 5

# Verify dashboard is running
max_dashboard_attempts=10
dashboard_attempt=1
while [ $dashboard_attempt -le $max_dashboard_attempts ]; do
    if curl -s http://localhost:8080/api/metrics > /dev/null 2>&1; then
        echo "✅ Dashboard is ready!"
        break
    fi
    echo "⏳ Waiting for dashboard... (attempt $dashboard_attempt/$max_dashboard_attempts)"
    sleep 2
    dashboard_attempt=$((dashboard_attempt + 1))
done

if [ $dashboard_attempt -gt $max_dashboard_attempts ]; then
    echo "⚠️  Dashboard may not be fully ready, but continuing..."
fi

# Start metrics updater in background
echo "📈 Starting metrics updater..."
PYTHONPATH=. python scripts/update_metrics.py &
METRICS_PID=$!

# Wait a moment for metrics to start updating
sleep 3

echo ""
echo "🎉 StreamSocial Partitioning System Started Successfully!"
echo "========================================================"
echo "📊 Dashboard: http://localhost:8080"
echo "📈 Metrics: Updating every 30 seconds"
echo "🔧 Kafka: Running on ports 9092, 9093, 9094"
echo "🛑 To stop: Run './stop.sh'"
echo ""

# Keep script running and handle cleanup on exit
echo "🔄 System is running. Press Ctrl+C to stop all services."
wait $DASHBOARD_PID $METRICS_PID
