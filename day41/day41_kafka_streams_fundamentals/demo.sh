#!/bin/bash

echo "====================================="
echo "Kafka Streams Fundamentals Demo"
echo "====================================="

# Get absolute path to current directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR"

# Activate virtual environment
if [ ! -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    echo "Error: Virtual environment not found. Please run setup.sh first."
    exit 1
fi
source "$SCRIPT_DIR/venv/bin/activate"

echo ""
echo "1. Starting services..."
"$SCRIPT_DIR/start.sh" &
START_PID=$!

# Wait for services to be ready
sleep 15

echo ""
echo "2. Checking health..."
curl -s http://localhost:5001/health | python -m json.tool

echo ""
echo "3. Query top scores..."
sleep 5
curl -s http://localhost:5001/scores/top?limit=10 | python -m json.tool

echo ""
echo "4. Query specific content score..."
curl -s http://localhost:5001/scores/post_1 | python -m json.tool

echo ""
echo "5. Get processing metrics..."
curl -s http://localhost:5001/metrics | python -m json.tool

echo ""
echo "====================================="
echo "Demo Complete!"
echo "====================================="
echo "Open http://localhost:5001/engagement_dashboard.html"
echo "to see real-time dashboard"
echo ""
echo "Press Enter to stop services..."
read

kill $START_PID 2>/dev/null || true
"$SCRIPT_DIR/stop.sh"
