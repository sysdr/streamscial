#!/bin/bash

echo "🛑 Stopping StreamSocial Rebalancing System..."

# Kill Python processes
pkill -f "python src/main.py" || true
pkill -f "prometheus_client" || true

# Stop Docker containers
cd docker
docker-compose down
cd ..

# Clean up temporary files
rm -f /tmp/consumer_count.txt

echo "✅ System stopped successfully!"
