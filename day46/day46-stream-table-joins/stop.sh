#!/bin/bash

echo "Stopping all services..."

# Kill Python processes
if [ -f .processor.pid ]; then
    kill $(cat .processor.pid) 2>/dev/null || true
    rm .processor.pid
fi

if [ -f .profile.pid ]; then
    kill $(cat .profile.pid) 2>/dev/null || true
    rm .profile.pid
fi

if [ -f .action.pid ]; then
    kill $(cat .action.pid) 2>/dev/null || true
    rm .action.pid
fi

if [ -f .dashboard.pid ]; then
    kill $(cat .dashboard.pid) 2>/dev/null || true
    rm .dashboard.pid
fi

# Stop Docker
docker-compose down

echo "All services stopped"
