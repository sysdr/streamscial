#!/bin/bash
# Stop script for StreamSocial Schema System

echo "🛑 Stopping StreamSocial Schema System..."

# Stop Docker services
docker-compose down

# Clean up
docker system prune -f

echo "✅ Services stopped successfully!"
