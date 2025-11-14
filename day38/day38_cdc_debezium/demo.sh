#!/bin/bash
set -e

echo "=========================================="
echo "CDC Demo - Automated Demonstration"
echo "=========================================="

# Activate virtual environment
source venv/bin/activate

# Start dashboard
echo "Starting monitoring dashboard..."
python dashboards/cdc_monitor.py &
DASHBOARD_PID=$!
sleep 3

# Start consumer
echo "Starting CDC consumer..."
python src/consumer/cdc_consumer.py &
CONSUMER_PID=$!
sleep 2

# Run producer simulation
echo "Starting profile update simulation..."
python src/producer/profile_updater.py

# Cleanup
echo "Cleaning up..."
kill $DASHBOARD_PID 2>/dev/null || true
kill $CONSUMER_PID 2>/dev/null || true

echo ""
echo "✓ Demo complete!"
echo "Check logs above for CDC events"
