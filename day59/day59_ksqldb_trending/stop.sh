#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "=== Stopping Day 59: ksqlDB Trending Analytics ==="

# Stop Python processes
if [ -f producer.pid ]; then
    kill $(cat producer.pid) 2>/dev/null
    rm producer.pid
fi

if [ -f dashboard.pid ]; then
    kill $(cat dashboard.pid) 2>/dev/null
    rm dashboard.pid
fi

# Stop Docker
cd docker
docker-compose down
cd ..

echo "✓ All services stopped"
