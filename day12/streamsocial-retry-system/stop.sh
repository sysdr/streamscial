#!/bin/bash

echo "🛑 Stopping StreamSocial Retry Logic System..."

# Kill Python processes
pkill -f "python src/main.py"

# Stop Docker containers
echo "🐳 Stopping Docker containers..."
cd docker
docker-compose down

echo "✅ StreamSocial Retry System stopped successfully!"
