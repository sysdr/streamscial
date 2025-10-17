#!/bin/bash

echo "🎬 Starting StreamSocial Avro Demo"

# Activate virtual environment
source venv_day27/bin/activate

# Start web dashboard in background
echo "📊 Starting web dashboard..."
cd dashboard && python app.py &
DASHBOARD_PID=$!

# Wait for dashboard to start
sleep 3

# Run demo
echo "🎭 Running Avro serialization demo..."
cd ..
python scripts/demo.py

echo "🌐 Dashboard available at: http://localhost:5000"
echo "🎯 Demo completed! Press Ctrl+C to stop dashboard"

# Keep dashboard running
wait $DASHBOARD_PID
