#!/bin/bash

echo "🛑 Stopping SASL Authentication System..."

# Stop dashboard
if [ -f .dashboard.pid ]; then
    PID=$(cat .dashboard.pid)
    kill $PID 2>/dev/null || true
    rm .dashboard.pid
    echo "✅ Dashboard stopped"
fi

# Stop Docker containers
docker-compose down

echo "✅ All services stopped"
