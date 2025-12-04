#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Stopping all services..."

if [ -f "$PROJECT_DIR/.pids" ]; then
    for pid in $(cat "$PROJECT_DIR/.pids"); do
        if ps -p "$pid" > /dev/null 2>&1; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    rm "$PROJECT_DIR/.pids"
fi

cd "$PROJECT_DIR/docker" && docker-compose down && cd "$PROJECT_DIR"

echo "All services stopped."

