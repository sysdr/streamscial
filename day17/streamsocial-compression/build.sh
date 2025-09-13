#!/bin/bash

echo "🔨 Building StreamSocial Compression Lab"
echo "======================================"

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Build Docker image
echo "🐳 Building Docker image..."
cd docker
docker-compose build
cd ..

echo "✅ Build completed successfully!"
echo ""
echo "Next steps:"
echo "  • Run: ./start.sh to start the application"
echo "  • Visit: http://localhost:8080 to access the dashboard"
echo "  • Run: ./stop.sh to stop all services"
