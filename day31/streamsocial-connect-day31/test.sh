#!/bin/bash
set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🧪 Running StreamSocial Connect tests..."

# Activate virtual environment
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "❌ Virtual environment not found. Please run build.sh first."
    exit 1
fi

source "$SCRIPT_DIR/venv/bin/activate"

# Wait for services to be ready
echo "⏳ Waiting for services..."
sleep 10

# Run unit tests
echo "🔬 Running unit tests..."
python -m pytest "$SCRIPT_DIR/tests/test_connect_cluster.py" -v

# Run integration tests  
echo "🔗 Running integration tests..."
python -m pytest "$SCRIPT_DIR/tests/test_integration.py" -v

echo "✅ All tests completed!"
