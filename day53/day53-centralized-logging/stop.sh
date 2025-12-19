#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Stopping all services..."

# Stop Python processes
pkill -f dashboard_app.py
pkill -f viral_content_producer.py
pkill -f engagement_consumer.py

# Stop Docker containers if docker-compose.yml exists
if [ -f "docker-compose.yml" ]; then
    docker-compose down
else
    echo "Warning: docker-compose.yml not found, skipping Docker cleanup"
fi

echo "All services stopped!"
