#!/bin/bash

echo "Stopping Interactive Queries system..."

# Kill application instances
if [ -f .pids ]; then
    while read pid; do
        kill $pid 2>/dev/null
    done < .pids
    rm .pids
fi

# Stop Docker
docker-compose down

echo "System stopped"
