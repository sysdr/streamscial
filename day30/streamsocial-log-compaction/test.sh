#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🧪 Running StreamSocial Log Compaction Tests..."

# Check if virtual environment exists
VENV_PATH="$SCRIPT_DIR/venv"
if [ ! -d "$VENV_PATH" ]; then
    echo "❌ Error: Virtual environment not found. Please run ./build.sh first"
    exit 1
fi

# Check if docker-compose.yml exists
if [ ! -f "$SCRIPT_DIR/docker-compose.yml" ]; then
    echo "❌ Error: docker-compose.yml not found in $SCRIPT_DIR"
    exit 1
fi

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Start Kafka for testing
echo "🚀 Starting Kafka for tests..."
docker-compose -f "$SCRIPT_DIR/docker-compose.yml" up -d
sleep 10

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
timeout 60s bash -c 'while ! docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do sleep 2; done'

# Check if test directories exist
if [ ! -d "$SCRIPT_DIR/tests/unit" ]; then
    echo "❌ Error: tests/unit directory not found"
    exit 1
fi

if [ ! -d "$SCRIPT_DIR/tests/integration" ]; then
    echo "❌ Error: tests/integration directory not found"
    exit 1
fi

# Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$SCRIPT_DIR"

# Run unit tests
echo "🔧 Running unit tests..."
python -m pytest "$SCRIPT_DIR/tests/unit/" -v

# Run integration tests
echo "🔗 Running integration tests..."
python -m pytest "$SCRIPT_DIR/tests/integration/" -v --timeout=60

echo "✅ All tests passed!"
