#!/bin/bash

echo "Stopping StreamSocial ML Training Data Sync..."

# Kill Python processes
pkill -f "uvicorn" || true
pkill -f "python.*streamsocial" || true

# Stop Docker services
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/docker"
docker compose down || docker-compose down

echo "All services stopped"
