#!/bin/bash
set -e

source venv/bin/activate

# Start services in background
echo "🚀 Starting StreamSocial services..."

# Start Kafka (if not running)
if ! docker ps | grep -q kafka; then
    docker run -d --name kafka-day23 \
        -p 9092:9092 \
        -e KAFKA_NODE_ID=1 \
        -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
        -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
        apache/kafka:latest
    
    echo "⏳ Waiting for Kafka to start..."
    sleep 10
fi

# Start Redis
if ! docker ps | grep -q redis-day23; then
    docker run -d --name redis-day23 -p 6379:6379 redis:7-alpine
fi

# Start the application
python src/main.py &
APP_PID=$!

# Start web dashboard
cd web && python app.py &
WEB_PID=$!

echo "✅ Services started successfully"
echo "🌐 Dashboard: http://localhost:5000"
echo "📊 Metrics: http://localhost:8080/metrics"
echo "❤️ Health: http://localhost:8080/health"

# Store PIDs for stop.sh
echo $APP_PID > .app.pid
echo $WEB_PID > .web.pid
