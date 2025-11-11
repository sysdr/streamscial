#!/bin/bash

echo "🚀 Starting Connect Monitoring System"

cd "$(dirname "$0")"
source venv/bin/activate

# Start PostgreSQL if not running
if ! pg_isready -h localhost -p 5432 > /dev/null 2>&1; then
    if command -v docker-compose > /dev/null 2>&1; then
        echo "Starting PostgreSQL with Docker..."
        docker-compose up -d postgres
        sleep 5
    else
        echo "⚠️ PostgreSQL not reachable and docker-compose not available; proceeding without database persistence."
    fi
fi

# Start monitoring service in background
echo "Starting monitoring service..."
python src/monitoring/monitor_service.py &
MONITOR_PID=$!
echo $MONITOR_PID > .monitor.pid

# Start dashboard
echo "Starting dashboard..."
python src/monitoring/dashboard.py &
DASHBOARD_PID=$!
echo $DASHBOARD_PID > .dashboard.pid

echo ""
echo "✅ System started!"
echo "📊 Dashboard: http://localhost:5000"
echo "📈 Prometheus metrics: http://localhost:8000"
echo ""
echo "Monitor PID: $MONITOR_PID"
echo "Dashboard PID: $DASHBOARD_PID"
