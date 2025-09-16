#!/bin/bash

# StreamSocial Transactional Producers - Start Script

set -e

echo "🚀 Starting StreamSocial Transactional Producers..."

# Ensure Kafka is running
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 15

# Activate virtual environment
source venv/bin/activate

echo "🌐 Starting StreamSocial API server..."
python -m src.streamsocial.api.app &
SERVER_PID=$!

echo "📊 StreamSocial Dashboard available at: http://localhost:8080"
echo "🔗 API endpoints:"
echo "  - POST /api/posts - Create atomic posts"  
echo "  - POST /api/posts/follower-sync - Multi-user timeline sync"
echo "  - GET /api/users - Get users list"
echo "  - GET /api/metrics - System metrics"
echo "  - POST /api/consume - Trigger message consumption"
echo ""
echo "Press Ctrl+C to stop the server"

# Wait for server process
wait $SERVER_PID
