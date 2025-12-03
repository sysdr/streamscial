#!/bin/bash

echo "Stopping StreamSocial Content Moderation Pipeline..."

# Kill Python processes
pkill -f "python src/stream_processor.py" || true
pkill -f "python web/dashboard.py" || true

# Stop Docker services
cd docker
docker-compose down
cd ..

echo "✓ Pipeline stopped"
