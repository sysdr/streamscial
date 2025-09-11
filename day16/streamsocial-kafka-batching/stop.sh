#!/bin/bash

echo "🛑 Stopping StreamSocial Kafka Batching System..."

# Stop Flask dashboard
pkill -f "python dashboard/app.py"

# Stop Docker services
cd docker && docker-compose down && cd ..

echo "✅ All services stopped!"

