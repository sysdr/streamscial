#!/bin/bash

echo "🚀 Starting StreamSocial Consumer Groups Demo"

# Start infrastructure
echo "📦 Starting Kafka infrastructure..."
cd docker && docker-compose up -d
cd ..

# Wait for services
echo "⏳ Waiting for services to be ready..."
sleep 30

# Activate virtual environment
source venv/bin/activate

# Create Kafka topics
echo "📝 Creating Kafka topics..."
docker exec broker kafka-topics --create --topic user-activities --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic generated-feeds --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec broker kafka-topics --create --topic consumer-metrics --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

# Start monitoring dashboard
echo "📊 Starting monitoring dashboard..."
python -m src.monitoring.dashboard &
DASHBOARD_PID=$!

# Wait for dashboard
sleep 5

# Run tests
echo "🧪 Running tests..."
pytest tests/ -v

# Start activity producer
echo "📤 Starting activity producer..."
python -m src.producers.activity_producer 500 300 &
PRODUCER_PID=$!

# Start consumer scaling demo
echo "🔄 Starting consumer scaling demonstration..."
python -m src.utils.consumer_manager &
MANAGER_PID=$!

echo ""
echo "✅ Demo is running!"
echo "🌐 Dashboard: http://localhost:8000"
echo "🔧 Kafka UI: http://localhost:8080"
echo ""
echo "Press Ctrl+C to stop all services"

# Store PIDs for cleanup
echo $DASHBOARD_PID > .dashboard.pid
echo $PRODUCER_PID > .producer.pid
echo $MANAGER_PID > .manager.pid

wait
