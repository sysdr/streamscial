#!/bin/bash

set -e

echo "======================================"
echo "   StreamSocial Sink Connector Demo   "
echo "======================================"

# Activate virtual environment
source venv/bin/activate

# Start infrastructure if not running
if ! docker-compose -f docker/docker-compose.yml ps | grep -q "Up"; then
    echo "Starting infrastructure..."
    ./start.sh
fi

# Start data producer in background
echo "Starting test data producer..."
python src/streamsocial/utils/test_data_producer.py &
PRODUCER_PID=$!

# Give producer time to start
sleep 3

# Start sink connector in background
echo "Starting sink connector..."
python src/main.py &
CONNECTOR_PID=$!

# Give connector time to start
sleep 10

echo ""
echo "======================================"
echo "           Demo Running!              "
echo "======================================"
echo ""
echo "🌐 Dashboard: http://localhost:5000"
echo "📊 Watch real-time hashtag analytics"
echo "🚀 Data is flowing from Kafka to PostgreSQL"
echo ""
echo "Press Ctrl+C to stop the demo..."
echo ""

# Wait for user to stop
trap 'echo "Stopping demo..."; kill $PRODUCER_PID $CONNECTOR_PID 2>/dev/null || true; exit' INT

# Keep demo running
while true; do
    sleep 1
done
