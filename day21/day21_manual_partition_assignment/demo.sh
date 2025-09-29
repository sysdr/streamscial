#!/bin/bash

echo "🎮 Running Day 21 Demo: Manual Partition Assignment"

# Activate virtual environment
source venv/bin/activate
export PYTHONPATH=$PWD:$PYTHONPATH

echo "Starting demo components..."

# Start Kafka and Redis in background
cd docker && docker-compose up -d zookeeper kafka redis && cd ..
echo "Waiting for services..."
sleep 10

# Start the main system in background
python -m src.main &
MAIN_PID=$!
sleep 5

# Start web dashboard in background
cd web && python dashboard.py &
WEB_PID=$!
cd ..
sleep 3

echo ""
echo "🎯 Demo is running!"
echo "🌐 Dashboard: http://localhost:5000"
echo ""
echo "What you should see:"
echo "  ✅ 3 trend workers processing different partitions"
echo "  ✅ Real-time hashtag trends updating every 5 seconds"
echo "  ✅ Manual partition assignment ensuring state consistency"
echo "  ✅ Trending hashtags: #kafka, #streaming, #python, etc."
echo ""
echo "🔍 Key behaviors to observe:"
echo "  • Each worker maintains independent trend state"
echo "  • Hashtags consistently route to same partitions"
echo "  • State persists across worker restarts"
echo "  • Real-time trend velocity calculations"
echo ""
echo "Press Ctrl+C to stop demo..."

# Wait for user interrupt
trap "echo 'Stopping demo...'; kill $MAIN_PID $WEB_PID; cd docker; docker-compose down; exit" INT
wait
