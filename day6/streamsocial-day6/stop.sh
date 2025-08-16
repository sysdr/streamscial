#!/bin/bash

echo "🛑 Stopping StreamSocial Consumer Groups Demo"

# Kill background processes
if [ -f .dashboard.pid ]; then
    kill $(cat .dashboard.pid) 2>/dev/null
    rm .dashboard.pid
fi

if [ -f .producer.pid ]; then
    kill $(cat .producer.pid) 2>/dev/null
    rm .producer.pid
fi

if [ -f .manager.pid ]; then
    kill $(cat .manager.pid) 2>/dev/null
    rm .manager.pid
fi

# Stop all consumer processes
pkill -f "src.consumers.feed_consumer"

# Stop Docker services
cd docker && docker-compose down
cd ..

echo "✅ All services stopped"
