#!/bin/bash
set -e

source venv/bin/activate

echo "🎬 Starting Day 34 SMT Pipeline Demo..."
echo "========================================"

# Start web dashboard in background
echo "Starting web dashboard..."
python src/web_server.py > logs/dashboard.log 2>&1 &
DASHBOARD_PID=$!
sleep 3

# Start pipeline orchestrator in background
echo "Starting SMT pipeline orchestrator..."
python src/pipeline_orchestrator.py > logs/pipeline.log 2>&1 &
PIPELINE_PID=$!
sleep 3

# Start producers
echo ""
echo "🎯 Starting data producers..."
echo "----------------------------"

echo "📱 Starting iOS producer..."
python src/producers/ios_producer.py > logs/ios_producer.log 2>&1 &
IOS_PID=$!

sleep 2

echo "🤖 Starting Android producer..."
python src/producers/android_producer.py > logs/android_producer.log 2>&1 &
ANDROID_PID=$!

sleep 2

echo "🌐 Starting Web producer..."
python src/producers/web_producer.py > logs/web_producer.log 2>&1 &
WEB_PID=$!

echo ""
echo "✅ All producers started!"
echo ""
echo "📊 Monitoring transformations for 30 seconds..."
echo "🌐 Open dashboard at: http://localhost:5000"
echo ""

# Monitor for 30 seconds
for i in {1..30}; do
    echo -n "."
    sleep 1
done

echo ""
echo ""
echo "📊 Consuming normalized events..."
python src/consumers/normalized_consumer.py

echo ""
echo "✅ Demo complete!"
echo ""
echo "Pipeline Statistics:"
tail -20 logs/pipeline.log

# Keep services running
echo ""
echo "Services are still running. Dashboard: http://localhost:5000"
echo "Press Ctrl+C to stop all services or run: ./scripts/stop.sh"

wait
