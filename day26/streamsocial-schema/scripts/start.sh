#!/bin/bash
# Start script for StreamSocial Schema System

set -e

echo "🚀 Starting StreamSocial Schema System..."

# Start with Docker Compose
echo "🐳 Starting services with Docker..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check service health
echo "🏥 Checking service health..."
curl -f http://localhost:8001/health || echo "Schema registry not ready"
curl -f http://localhost:8501 || echo "Dashboard not ready"

echo "✅ Services started! Access dashboard at http://localhost:8501"
