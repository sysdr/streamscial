#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting StreamSocial Log Compaction Demo..."

# Check if virtual environment exists
VENV_PATH="$SCRIPT_DIR/venv"
if [ ! -d "$VENV_PATH" ]; then
    echo "❌ Error: Virtual environment not found. Please run ./build.sh first"
    exit 1
fi

# Check if src/dashboard.py exists
if [ ! -f "$SCRIPT_DIR/src/dashboard.py" ]; then
    echo "❌ Error: src/dashboard.py not found in $SCRIPT_DIR"
    exit 1
fi

# Check if docker-compose.yml exists
if [ ! -f "$SCRIPT_DIR/docker-compose.yml" ]; then
    echo "❌ Error: docker-compose.yml not found in $SCRIPT_DIR"
    exit 1
fi

# Check for existing dashboard processes
EXISTING_PID=$(pgrep -f "python.*dashboard" | grep -v $$ || true)
if [ -n "$EXISTING_PID" ]; then
    echo "⚠️  Warning: Existing dashboard process found (PID: $EXISTING_PID)"
    echo "   Stopping existing process..."
    pkill -f "python.*dashboard" || true
    sleep 2
fi

# Start Kafka services
echo "🐘 Starting Kafka services..."
docker-compose -f "$SCRIPT_DIR/docker-compose.yml" up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check if Kafka is ready - wait up to 60 seconds
echo "⏳ Verifying Kafka is ready (this may take up to 60 seconds)..."
MAX_WAIT=60
WAIT_COUNT=0
KAFKA_READY=false

while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        # Additional check: try to list topics
        if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
            KAFKA_READY=true
            break
        fi
    fi
    echo "   Waiting for Kafka... (${WAIT_COUNT}s/${MAX_WAIT}s)"
    sleep 2
    WAIT_COUNT=$((WAIT_COUNT + 2))
done

if [ "$KAFKA_READY" = false ]; then
    echo "❌ Error: Kafka did not become ready within ${MAX_WAIT} seconds"
    echo "   You may need to wait longer or check Docker logs: docker logs kafka"
    exit 1
fi

echo "✅ Kafka is ready!"

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Set Flask environment
export FLASK_ENV=development
export PYTHONPATH="${PYTHONPATH}:$SCRIPT_DIR"

# Load .env if it exists
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(cat "$SCRIPT_DIR/.env" | grep -v '^#' | xargs)
fi

echo "🌐 Starting web dashboard..."
echo "📱 Dashboard will be available at: http://localhost:5000"
echo "🔧 Kafka UI available at: http://localhost:8080"
echo ""
echo "🎯 Demo Features:"
echo "   • Real-time preference updates"
echo "   • Log compaction visualization"
echo "   • User simulation"
echo "   • State rebuilding"
echo "   • Tombstone deletion demo"
echo ""
echo "Press Ctrl+C to stop..."

python -m src.dashboard
