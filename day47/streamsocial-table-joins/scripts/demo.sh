#!/bin/bash

echo "=================================================="
echo "Day 47: Table-Table Joins Demo"
echo "=================================================="

# Start system
./scripts/start.sh &
START_PID=$!

# Wait for system to initialize
echo "Initializing system..."
sleep 20

echo ""
echo "System is running!"
echo "Opening dashboard in browser..."
echo ""
echo "📊 Dashboard: http://localhost:5000"
echo ""
echo "What you should see:"
echo "- Join operations counter increasing"
echo "- Recommendations being generated in real-time"
echo "- Score distribution showing match quality"
echo "- Recent recommendations with match details"
echo ""
echo "The system is:"
echo "1. Joining user preference updates with content metadata"
echo "2. Computing match scores for each user-content pair"
echo "3. Generating recommendations above 0.5 score threshold"
echo ""
echo "Press Ctrl+C to stop the demo"

wait $START_PID
