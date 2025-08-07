#!/bin/bash

echo "🛑 Stopping StreamSocial Producer Demo"
echo "====================================="

# Kill any running Python processes
pkill -f "python.*demo.py" || true
pkill -f "python.*dashboard.py" || true
pkill -f "python.*metrics_server.py" || true

# Stop Kafka
echo "🐳 Stopping Kafka cluster..."
cd docker
docker-compose down
cd ..

# Deactivate virtual environment
deactivate 2>/dev/null || true

echo "✅ All services stopped"
