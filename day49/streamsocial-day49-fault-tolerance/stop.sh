#!/bin/bash

echo "Stopping StreamSocial Fault Tolerance Demo..."

if [ -f /tmp/dashboard.pid ]; then
    kill $(cat /tmp/dashboard.pid) 2>/dev/null
    rm /tmp/dashboard.pid
fi

if [ -f /tmp/processor.pid ]; then
    kill $(cat /tmp/processor.pid) 2>/dev/null
    rm /tmp/processor.pid
fi

pkill -f "python src/"

echo "✓ Stopped"
