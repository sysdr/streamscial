#!/bin/bash

echo "🛑 Stopping StreamSocial system..."

# Stop Python processes
pkill -f "python src/dashboard.py" 2>/dev/null || true

# Stop Docker containers
cd docker
docker-compose down
cd ..

# Deactivate virtual environment
deactivate 2>/dev/null || true

echo "✅ StreamSocial system stopped."
