#!/bin/bash

echo "🛑 Stopping StreamSocial Producer Demo"
echo "======================================"

# Kill any running Python processes
pkill -f "python src/main.py" || true

# Stop Docker Compose
echo "🐳 Stopping Kafka cluster..."
docker-compose down

echo "✅ StreamSocial Producer Demo stopped successfully"
