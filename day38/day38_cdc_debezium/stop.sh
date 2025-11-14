#!/bin/bash

echo "Stopping all services..."

# Kill Python processes
pkill -f "cdc_monitor.py" || true
pkill -f "cdc_consumer.py" || true
pkill -f "profile_updater.py" || true

# Stop Docker containers
cd docker
docker-compose down

echo "✓ All services stopped"
