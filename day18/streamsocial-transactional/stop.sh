#!/bin/bash

# StreamSocial Transactional Producers - Stop Script

echo "🛑 Stopping StreamSocial Transactional Producers..."

# Kill Python processes
pkill -f "python.*src.streamsocial.api.app" || true

echo "🐳 Stopping Kafka cluster..."
docker-compose down

echo "✅ StreamSocial stopped successfully!"
