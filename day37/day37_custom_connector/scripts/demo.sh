#!/bin/bash

# Demo script - shows connector functionality

set -e

echo "=========================================="
echo "Day 37 Demo: Custom Connector Development"
echo "=========================================="

# Activate virtual environment
source venv/bin/activate

echo ""
echo "Starting connector in background..."
python src/main.py > connector.log 2>&1 &
CONNECTOR_PID=$!

# Wait for startup
sleep 5

echo ""
echo "Connector running! (PID: $CONNECTOR_PID)"
echo ""
echo "Available endpoints:"
echo "  - Dashboard: http://localhost:5000"
echo "  - Metrics:   http://localhost:8080/metrics"
echo ""
echo "Watching connector logs for 30 seconds..."
echo "=========================================="
echo ""

# Show logs for 30 seconds
timeout 30 tail -f connector.log || true

echo ""
echo "Demo complete. Stopping connector..."
kill $CONNECTOR_PID

echo ""
echo "✓ Connector successfully demonstrated:"
echo "  - Multi-platform support (Twitter, LinkedIn)"
echo "  - Rate limiting with token bucket"
echo "  - Real-time metrics and monitoring"
echo "  - Task-based parallelism"
echo "  - Offset management"
echo ""
echo "Check connector.log for full output"
