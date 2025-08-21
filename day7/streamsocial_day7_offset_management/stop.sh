#!/bin/bash

echo "🛑 Stopping StreamSocial Offset Management System..."

# Kill application processes
if [ -f .data_generator.pid ]; then
    kill $(cat .data_generator.pid) 2>/dev/null || true
    rm .data_generator.pid
fi

if [ -f .dashboard.pid ]; then
    kill $(cat .dashboard.pid) 2>/dev/null || true
    rm .dashboard.pid
fi

if [ -f .processor.pid ]; then
    kill $(cat .processor.pid) 2>/dev/null || true
    rm .processor.pid
fi

# Stop Docker services
echo "🐳 Stopping infrastructure services..."
docker-compose down

# Deactivate virtual environment
if [ -n "$VIRTUAL_ENV" ]; then
    deactivate
fi

echo "✅ StreamSocial Offset Management System stopped"
