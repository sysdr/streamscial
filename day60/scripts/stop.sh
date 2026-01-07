#!/bin/bash

echo "Stopping StreamSocial Production System..."

if [ -f logs/dashboard.pid ]; then
    PID=$(cat logs/dashboard.pid)
    kill $PID 2>/dev/null || true
    rm logs/dashboard.pid
    echo "✓ Dashboard stopped"
else
    echo "No running dashboard found"
fi
