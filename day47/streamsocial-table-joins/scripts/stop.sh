#!/bin/bash

echo "Stopping Table-Table Joins System..."

# Kill processes
if [ -f /tmp/dashboard.pid ]; then
    kill $(cat /tmp/dashboard.pid) 2>/dev/null
    rm /tmp/dashboard.pid
fi

if [ -f /tmp/producer.pid ]; then
    kill $(cat /tmp/producer.pid) 2>/dev/null
    rm /tmp/producer.pid
fi

if [ -f /tmp/processor.pid ]; then
    kill $(cat /tmp/processor.pid) 2>/dev/null
    rm /tmp/processor.pid
fi

# Stop Docker
docker-compose down

echo "System stopped!"
