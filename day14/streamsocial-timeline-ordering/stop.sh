#!/bin/bash

# StreamSocial Timeline Ordering Stop Script

echo "🛑 Stopping StreamSocial Timeline Ordering System"
echo "=============================================="

echo "📱 Stopping web application..."
pkill -f "uvicorn src.web.dashboard:app" || true

echo "🐳 Stopping Docker containers..."
docker-compose -f docker/docker-compose.yml down

echo "🧹 Cleaning up..."
docker system prune -f --volumes || true

echo "✅ System stopped successfully!"
