#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Change to project root
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Starting Schema Governance Framework"
echo "=========================================="

# Activate virtual environment
source venv/bin/activate

# Start dashboard in background
echo "Starting dashboard on http://localhost:5058..."
python dashboards/governance_dashboard.py &
DASHBOARD_PID=$!

# Wait for dashboard to start
sleep 3

# Start metrics collector in background
echo "Starting metrics collector service..."
python -m src.monitoring.metrics_collector &
COLLECTOR_PID=$!

# Wait for collector to initialize
sleep 2

# Open dashboard in browser (if available)
if command -v xdg-open &> /dev/null; then
    xdg-open http://localhost:5058
elif command -v open &> /dev/null; then
    open http://localhost:5058
fi

echo ""
echo "✓ Dashboard started!"
echo "  Access at: http://localhost:5058"
echo ""
echo "✓ Metrics collector started!"
echo ""
echo "Dashboard PID: $DASHBOARD_PID"
echo "Collector PID: $COLLECTOR_PID"
echo "To stop: kill $DASHBOARD_PID $COLLECTOR_PID"
