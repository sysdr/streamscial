#!/bin/bash

echo "Stopping StreamSocial Day 40 services..."

# Kill PIDs
if [ -f /tmp/connector.pid ]; then
    kill $(cat /tmp/connector.pid) 2>/dev/null || true
    rm /tmp/connector.pid
fi

if [ -f /tmp/dlq.pid ]; then
    kill $(cat /tmp/dlq.pid) 2>/dev/null || true
    rm /tmp/dlq.pid
fi

if [ -f /tmp/dashboard.pid ]; then
    kill $(cat /tmp/dashboard.pid) 2>/dev/null || true
    rm /tmp/dashboard.pid
fi

# Stop Docker
if command -v docker-compose &> /dev/null; then
    cd docker
    docker-compose down
    cd ..
fi

echo "✓ All services stopped"
