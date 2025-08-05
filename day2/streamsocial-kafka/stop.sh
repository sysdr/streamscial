#!/bin/bash

echo "🛑 Stopping StreamSocial Kafka Environment..."

# Stop monitoring dashboard
if [ -f .monitor.pid ]; then
    MONITOR_PID=$(cat .monitor.pid)
    if ps -p $MONITOR_PID > /dev/null; then
        echo "🛑 Stopping monitoring dashboard (PID: $MONITOR_PID)"
        kill $MONITOR_PID 2>/dev/null || true
    fi
    rm -f .monitor.pid
else
    echo "🔄 Stopping any monitoring dashboard processes..."
    pkill -f "monitoring_dashboard.py" 2>/dev/null || true
fi

# Stop frontend
if [ -f .frontend.pid ]; then
    FRONTEND_PID=$(cat .frontend.pid)
    if ps -p $FRONTEND_PID > /dev/null; then
        echo "🛑 Stopping frontend (PID: $FRONTEND_PID)"
        kill $FRONTEND_PID 2>/dev/null || true
    fi
    rm -f .frontend.pid
else
    echo "🔄 Stopping any frontend processes..."
    pkill -f "http.server" 2>/dev/null || true
fi

# Stop Docker services
echo "🛑 Stopping Docker services..."
docker compose -f docker/docker-compose.yml down 2>/dev/null || true

# Clean up any remaining processes
echo "🧹 Cleaning up any remaining processes..."
pkill -f "python.*monitoring_dashboard" 2>/dev/null || true
pkill -f "python.*http.server" 2>/dev/null || true

echo "✅ StreamSocial Kafka Environment stopped"
