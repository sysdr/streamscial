#!/bin/bash

set -e

echo "🔨 Building StreamSocial Low-Latency Consumer"

# Activate virtual environment
source venv/bin/activate

echo "🧪 Running tests..."
python -m pytest tests/ -v

echo "🐳 Building Docker image..."
cd docker
docker-compose build

echo "✅ Build completed successfully!"
echo ""
echo "Next steps:"
echo "  1. Run './start.sh' to start all services"
echo "  2. Open http://localhost:5000 for dashboard"
echo "  3. Use './scripts/load_test.sh' for performance testing"
