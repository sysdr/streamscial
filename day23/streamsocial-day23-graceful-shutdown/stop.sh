#!/bin/bash

echo "🛑 Stopping StreamSocial services..."

# Kill application processes
if [ -f .app.pid ]; then
    kill $(cat .app.pid) 2>/dev/null || true
    rm .app.pid
fi

if [ -f .web.pid ]; then
    kill $(cat .web.pid) 2>/dev/null || true
    rm .web.pid
fi

# Stop Docker containers
docker stop kafka-day23 redis-day23 2>/dev/null || true
docker rm kafka-day23 redis-day23 2>/dev/null || true

echo "✅ Services stopped"
