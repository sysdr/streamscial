#!/bin/bash
set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🏗️  Building StreamSocial Connect Architecture..."

# Create and activate virtual environment
echo "🐍 Setting up Python virtual environment..."
if [ ! -d "venv" ]; then
    python3.11 -m venv venv
fi
source "$SCRIPT_DIR/venv/bin/activate"

# Install Python dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install -r "$SCRIPT_DIR/requirements.txt"

# Build Docker images
echo "🐳 Building Docker images..."
cd "$SCRIPT_DIR"
docker-compose build --no-cache

echo "✅ Build completed successfully!"
echo ""
echo "Next steps:"
echo "  $SCRIPT_DIR/start.sh    - Start the Connect cluster"
echo "  $SCRIPT_DIR/test.sh     - Run tests"
echo "  $SCRIPT_DIR/stop.sh     - Stop all services"
