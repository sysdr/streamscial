#!/bin/bash

echo "Stopping all services..."

# Kill service processes
if [ -d pids ]; then
    for pidfile in pids/*.pid; do
        if [ -f "$pidfile" ]; then
            kill $(cat "$pidfile") 2>/dev/null || true
            rm "$pidfile"
        fi
    done
fi

# Stop Docker services
cd docker
docker-compose down
cd ..

echo "✓ All services stopped"
