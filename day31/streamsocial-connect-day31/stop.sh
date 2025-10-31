#!/bin/bash

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🛑 Stopping StreamSocial Connect Cluster..."

# Stop data generator
if [ -f "$SCRIPT_DIR/data_generator.pid" ]; then
    PID=$(cat "$SCRIPT_DIR/data_generator.pid")
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID 2>/dev/null || true
    fi
    rm -f "$SCRIPT_DIR/data_generator.pid"
fi

# Stop all Docker services
cd "$SCRIPT_DIR"
docker-compose down

echo "✅ All services stopped"
