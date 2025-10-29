#!/bin/bash

echo "🛑 Stopping StreamSocial Log Compaction Demo..."

# Kill Python processes
pkill -f "python -m src.dashboard" || true
pkill -f "python.*dashboard" || true

# Stop Docker services
docker-compose down

echo "✅ Demo stopped successfully!"
