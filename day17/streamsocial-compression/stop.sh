#!/bin/bash

echo "🛑 Stopping StreamSocial Compression Lab"
echo "====================================="

# Stop Docker services
if [ -f "docker/docker-compose.yml" ]; then
    echo "Stopping Docker services..."
    cd docker
    docker-compose down
    cd ..
fi

# Stop local processes
if [ -f ".app_pid" ]; then
    APP_PID=$(cat .app_pid)
    if ps -p $APP_PID > /dev/null; then
        echo "Stopping Flask application (PID: $APP_PID)..."
        kill $APP_PID
    fi
    rm .app_pid
fi

# Stop Redis
if pgrep redis-server > /dev/null; then
    echo "Stopping Redis..."
    pkill redis-server
fi

echo "✅ All services stopped"
