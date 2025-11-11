#!/bin/bash

echo "🛑 Stopping Connect Monitoring System"

cd "$(dirname "$0")"

# Stop services
if [ -f .monitor.pid ]; then
    kill $(cat .monitor.pid) 2>/dev/null || true
    rm .monitor.pid
fi

if [ -f .dashboard.pid ]; then
    kill $(cat .dashboard.pid) 2>/dev/null || true
    rm .dashboard.pid
fi

# Stop Docker services
if command -v docker-compose > /dev/null 2>&1; then
    docker-compose down
else
    echo "ℹ️ docker-compose not available; skipping container shutdown."
fi

echo "✅ System stopped"
