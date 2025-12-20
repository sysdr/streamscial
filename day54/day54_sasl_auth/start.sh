#!/bin/bash
set -e

echo "🚀 Starting SASL Authentication Monitor..."

# Activate virtual environment
source venv/bin/activate

# Start dashboard
echo "📊 Starting dashboard on http://localhost:8054..."
python dashboard/app.py &

DASHBOARD_PID=$!
echo $DASHBOARD_PID > .dashboard.pid

echo ""
echo "✅ Dashboard started!"
echo "   URL: http://localhost:8054"
echo "   PID: $DASHBOARD_PID"
echo ""
echo "Press Ctrl+C to stop..."
echo ""

# Wait for interrupt
wait $DASHBOARD_PID
