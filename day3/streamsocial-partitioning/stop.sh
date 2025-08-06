#!/bin/bash

echo "🛑 Stopping StreamSocial services..."



# Stop dashboard
echo "📊 Stopping dashboard..."
pkill -f "python src/dashboard.py"

# Stop metrics updater
echo "📈 Stopping metrics updater..."
pkill -f "python scripts/update_metrics.py"

# Stop any remaining Python processes related to our project
echo "🔍 Cleaning up remaining processes..."
pkill -f "streamsocial-partitioning" 2>/dev/null || true

# Stop Kafka cluster
echo "🔧 Stopping Kafka cluster..."
cd docker
docker-compose down

echo "✅ All services stopped"
echo "💡 If you still have port conflicts, run: lsof -ti:8080 | xargs kill -9"

# Check and kill processes using port 8080 (dashboard)
echo "🔍 Checking for processes using port 8080..."
if lsof -ti:8080 > /dev/null 2>&1; then
    echo "⚠️  Found processes using port 8080. Stopping them..."
    lsof -ti:8080 | xargs kill -9
    sleep 2
fi

# Check and kill processes using Kafka ports
echo "🔍 Checking for processes using Kafka ports..."
for port in 9092 9093 9094 2181; do
    if lsof -ti:$port > /dev/null 2>&1; then
        echo "⚠️  Found processes using port $port. Stopping them..."
        lsof -ti:$port | xargs kill -9
        sleep 1
    fi
done