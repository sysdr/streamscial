#!/bin/bash

echo "🎬 StreamSocial Low-Latency Consumer Demo"
echo "=========================================="

# Activate virtual environment
source venv/bin/activate

# Set Python path to include project root
export PYTHONPATH=$(pwd):$PYTHONPATH

echo ""
echo "📊 Dashboard is running at: http://localhost:5000"
echo "📈 Metrics endpoint: http://localhost:8080/metrics"
echo ""

echo "🔥 Starting load test (1000 msg/sec for 30 seconds)..."
python src/utils/test_producer.py load 1000 30 &

echo ""
echo "📱 Sending individual test notifications..."
for type in critical important standard; do
    echo "Sending $type notification..."
    docker-compose -f docker/docker-compose.yml exec kafka kafka-console-producer --topic ${type}-notifications --bootstrap-server localhost:9092 <<< "{\"id\":\"demo_$type\",\"type\":\"$type\",\"user_id\":\"demo_user\",\"timestamp\":$(date +%s),\"title\":\"Demo $type notification\"}"
done

echo ""
echo "🎯 Monitor the dashboard to see sub-50ms latencies!"
echo "💡 Check Docker logs: docker-compose -f docker/docker-compose.yml logs -f consumer"
