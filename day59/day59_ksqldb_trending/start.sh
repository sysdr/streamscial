#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "=== Starting Day 59: ksqlDB Trending Analytics ==="

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "ERROR: Virtual environment not found. Please run setup.sh first."
    exit 1
fi

# Check if services are already running
if [ -f "producer.pid" ] && ps -p $(cat producer.pid) > /dev/null 2>&1; then
    echo "WARNING: Event producer is already running (PID: $(cat producer.pid))"
fi

if [ -f "dashboard.pid" ] && ps -p $(cat dashboard.pid) > /dev/null 2>&1; then
    echo "WARNING: Dashboard is already running (PID: $(cat dashboard.pid))"
fi

source venv/bin/activate

# Start event producer in background
echo "Starting event producer..."
python src/event_producer.py &
PRODUCER_PID=$!
echo $PRODUCER_PID > producer.pid

# Start dashboard
echo "Starting dashboard on http://localhost:5000..."
nohup python ui/dashboard.py > dashboard.log 2>&1 &
DASHBOARD_PID=$!
echo $DASHBOARD_PID > dashboard.pid
sleep 3
if ! ps -p $DASHBOARD_PID > /dev/null 2>&1; then
    echo "ERROR: Dashboard failed to start. Check dashboard.log for errors."
    cat dashboard.log 2>/dev/null || true
    exit 1
fi

echo "✓ System started!"
echo ""
echo "Dashboard: http://localhost:5000"
echo "ksqlDB Server: http://localhost:8088"
echo ""
echo "Press Ctrl+C to stop, or run ./stop.sh"
