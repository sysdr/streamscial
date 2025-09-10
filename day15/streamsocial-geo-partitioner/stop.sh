#!/bin/bash
echo "🛑 Stopping StreamSocial services..."

# Kill background processes
if [ -f monitor.pid ]; then
    kill $(cat monitor.pid) 2>/dev/null || true
    rm monitor.pid
fi

if [ -f producer.pid ]; then
    kill $(cat producer.pid) 2>/dev/null || true
    rm producer.pid
fi

# Stop Docker containers
cd docker
docker-compose down
cd ..

echo "✅ All services stopped!"
