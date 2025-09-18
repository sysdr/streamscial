#!/bin/bash

echo "🛑 Stopping StreamSocial Async Producer..."

# Kill Python processes
pkill -f "python src/api_server.py"

# Stop Docker containers
cd docker
docker-compose down
cd ..

echo "✅ All services stopped"
