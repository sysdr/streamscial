#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=================================================="
echo "Day 53: Centralized Logging Demo"
echo "=================================================="

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "Error: venv directory not found in $SCRIPT_DIR"
    exit 1
fi

# Check if required Python files exist
if [ ! -f "src/dashboard_app.py" ]; then
    echo "Error: src/dashboard_app.py not found"
    exit 1
fi

if [ ! -f "src/consumers/engagement_consumer.py" ]; then
    echo "Error: src/consumers/engagement_consumer.py not found"
    exit 1
fi

if [ ! -f "src/producers/viral_content_producer.py" ]; then
    echo "Error: src/producers/viral_content_producer.py not found"
    exit 1
fi

# Activate virtual environment
source "$SCRIPT_DIR/venv/bin/activate"

# Ensure logs directory exists
mkdir -p "$SCRIPT_DIR/logs/application" "$SCRIPT_DIR/logs/kafka"

echo ""
echo "Starting monitoring dashboard..."
python "$SCRIPT_DIR/src/dashboard_app.py" &
DASHBOARD_PID=$!

# Wait for dashboard to start
sleep 5

echo ""
echo "Starting consumer..."
python "$SCRIPT_DIR/src/consumers/engagement_consumer.py" &
CONSUMER_PID=$!

# Wait for consumer to initialize
sleep 3

echo ""
echo "Starting producer (60 second simulation)..."
python "$SCRIPT_DIR/src/producers/viral_content_producer.py"

echo ""
echo "=================================================="
echo "Demo Results"
echo "=================================================="
echo ""
echo "Logs have been generated and shipped to ELK stack!"
echo ""
echo "Access the monitoring dashboard:"
echo "👉 http://localhost:5000"
echo ""
echo "Access Kibana for detailed log analysis:"
echo "👉 http://localhost:5601"
echo ""
echo "To explore Kibana:"
echo "1. Go to Analytics → Discover"
echo "2. Create index pattern: streamsocial-logs-*"
echo "3. Search for specific trace IDs"
echo "4. View error rates and latency metrics"
echo ""
echo "The dashboard shows:"
echo "  ✓ Real-time error rates"
echo "  ✓ Latency statistics (p50, p95, p99)"
echo "  ✓ Top error messages"
echo "  ✓ Trace ID search functionality"
echo ""
echo "Press Ctrl+C to stop the demo"
echo ""

# Keep demo running
wait

# Cleanup on exit
trap "kill $DASHBOARD_PID $CONSUMER_PID 2>/dev/null" EXIT
