#!/bin/bash

echo "🚀 Starting StreamSocial Engagement Consumer Demo"
echo "================================================"

# Activate virtual environment
source venv/bin/activate

# Start Docker services
echo "🐳 Starting Kafka and Redis..."
cd docker && docker-compose up -d
cd ..

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 15

# Create Kafka topic
echo "📝 Creating Kafka topic..."
docker exec kafka kafka-topics --create \
    --topic user-engagements \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo "✅ Kafka topic created successfully"

# Start Redis if not running
echo "🔄 Checking Redis..."
if ! docker ps | grep -q redis; then
    echo "Starting Redis..."
    docker run -d --name redis -p 6379:6379 redis:7.2.5-alpine
fi

echo "🌐 Starting Web Dashboard..."
export PYTHONPATH=$PWD
python src/web/dashboard.py &
WEB_PID=$!

echo "🎬 Starting Demo Data Producer..."
python src/utils/demo_producer.py &
PRODUCER_PID=$!

echo "🔥 Starting Engagement Consumer..."
python src/consumer/engagement_consumer.py &
CONSUMER_PID=$!

echo ""
echo "✅ All services started successfully!"
echo "================================================"
echo "📊 Dashboard: http://localhost:5000"
echo "📈 Kafka UI: Check docker logs for Kafka status"
echo "🔴 Redis: localhost:6379"
echo ""
echo "🎯 Demo is running with live engagement events!"
echo "   - Consumer processing engagement events"
echo "   - Web dashboard showing real-time stats"
echo "   - Demo producer generating test data"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for user interrupt
trap 'echo "🛑 Stopping services..."; kill $WEB_PID $PRODUCER_PID $CONSUMER_PID 2>/dev/null; cd docker && docker-compose down; echo "✅ All services stopped"; exit 0' INT

wait
