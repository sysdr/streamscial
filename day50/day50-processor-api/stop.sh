#!/bin/bash

echo "Stopping Day 50: Processor API System..."

# Kill Python processes
pkill -f "python src/main.py" || true
pkill -f "python3 -m http.server" || true

# Stop Docker containers
docker-compose down

echo "✅ System stopped"
