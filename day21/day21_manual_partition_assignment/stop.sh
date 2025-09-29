#!/bin/bash

echo "🛑 Stopping StreamSocial Trend Analysis System"

# Kill Python processes
pkill -f "python -m src.main"
pkill -f "python dashboard.py"

# Stop Docker services
if [ -f docker/docker-compose.yml ]; then
    cd docker
    docker-compose down
    cd ..
fi

echo "✅ System stopped!"
