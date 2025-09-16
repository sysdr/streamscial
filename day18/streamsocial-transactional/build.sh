#!/bin/bash

# StreamSocial Transactional Producers - Build Script

set -e

echo "🔨 Building StreamSocial Transactional Producers..."

# Activate virtual environment
source venv/bin/activate

echo "📦 Installing/updating dependencies..."
pip install -r requirements.txt

echo "🐳 Starting Kafka cluster..."
docker-compose up -d

echo "⏳ Waiting for Kafka cluster to be ready..."
sleep 30

echo "🧪 Running tests..."
python -m pytest tests/ -v --tb=short

echo "✅ Build completed successfully!"
echo ""
echo "Next steps:"
echo "  1. Run: ./start.sh to start the application"
echo "  2. Visit: http://localhost:8080 for the dashboard"
echo "  3. Run: ./stop.sh to stop everything"
