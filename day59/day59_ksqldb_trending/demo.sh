#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "=== Day 59: ksqlDB Trending Analytics - Full Demo ==="

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "ERROR: Virtual environment not found. Please run setup.sh first."
    exit 1
fi

source venv/bin/activate

echo ""
echo "🎯 Demo Scenario: Real-time trending hashtag detection"
echo "================================================"
echo ""

# Start services if not running
if ! pgrep -f "event_producer.py" > /dev/null; then
    echo "Starting event producer..."
    python src/event_producer.py &
    PRODUCER_PID=$!
    sleep 5
fi

if ! pgrep -f "dashboard.py" > /dev/null; then
    echo "Starting dashboard..."
    python ui/dashboard.py &
    DASHBOARD_PID=$!
    sleep 5
fi

echo "✓ Services running"
echo ""

# Show ksqlDB streams
echo "📊 ksqlDB Streams:"
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW STREAMS;"}' 2>/dev/null | python -m json.tool | head -20

echo ""
echo "📊 ksqlDB Tables:"
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW TABLES;"}' 2>/dev/null | python -m json.tool | head -20

echo ""
echo "🔥 Top Trending Hashtags (Real-time):"
sleep 10  # Let some events process

curl -s http://localhost:5000/api/trending/hashtags | python -m json.tool | head -30

echo ""
echo "🚀 Viral Posts:"
curl -s http://localhost:5000/api/viral/posts | python -m json.tool | head -30

echo ""
echo "✅ Demo Complete!"
echo ""
echo "📈 Dashboard running at: http://localhost:5000"
echo "🔧 ksqlDB REST API: http://localhost:8088"
echo ""
echo "Press Ctrl+C to exit"

# Keep running
wait
