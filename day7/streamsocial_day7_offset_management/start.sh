#!/bin/bash

echo "🚀 Starting StreamSocial Day 7: Offset Management System"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3.11 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Start infrastructure with Docker
echo "🐳 Starting infrastructure services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to initialize..."
sleep 20

# Check if services are ready
echo "🔍 Checking service health..."
until docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo "Waiting for Kafka..."
    sleep 5
done

until docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; do
    echo "Waiting for Redis..."
    sleep 2
done

until docker-compose exec -T postgres pg_isready -U admin > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 2
done

# Create Kafka topic
echo "📋 Creating Kafka topics..."
docker-compose exec -T kafka kafka-topics --create --topic user_engagement_events --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --if-not-exists

# Start web dashboard in background (run from project root so absolute imports work)
echo "🌐 Starting web dashboard..."
uvicorn src.web_dashboard.app:app --host 0.0.0.0 --port 8080 &
DASHBOARD_PID=$!

# Wait for dashboard to become ready
echo "🩺 Waiting for dashboard to be ready on http://localhost:8080 ..."
DASHBOARD_READY=false
for i in {1..30}; do
    if curl -fsS -m 2 http://localhost:8080/ > /dev/null 2>&1; then
        echo "✅ Dashboard is ready!"
        DASHBOARD_READY=true
        break
    fi
    echo "Waiting for dashboard... (attempt $i/30)"
    sleep 2
done

if [ "$DASHBOARD_READY" = false ]; then
    echo "❌ Dashboard failed to start within 60 seconds"
    echo "Checking dashboard logs..."
    ps aux | grep uvicorn | grep -v grep
    exit 1
fi

# Run tests (do not block bringing the rest of the stack up)
echo "🧪 Running tests..."
python -m pytest tests/ -v &
TESTS_PID=$!

# Start test data generator in background
echo "📊 Starting test data generator..."
python -c "
import asyncio
from src.engagement_processor.test_data_generator import TestDataGenerator
from config.app_config import AppConfig

async def generate():
    config = AppConfig()
    generator = TestDataGenerator(config)
    await generator.start_generating(events_per_second=20, duration_seconds=300)

asyncio.run(generate())
" &

DATA_GENERATOR_PID=$!

# Start main processor
echo "⚡ Starting offset management processor..."
python -m src.main &
PROCESSOR_PID=$!

echo "✅ StreamSocial Offset Management System started successfully!"
echo ""
echo "🌐 Web Dashboard: http://localhost:8080"
echo "📊 Monitor logs: tail -f logs/app.log"
echo ""
echo "To stop the system, run: ./stop.sh"

# Save PIDs for cleanup
echo $DATA_GENERATOR_PID > .data_generator.pid
echo $DASHBOARD_PID > .dashboard.pid
echo ${TESTS_PID:-} > .tests.pid
echo $PROCESSOR_PID > .processor.pid

# Keep script running
wait
