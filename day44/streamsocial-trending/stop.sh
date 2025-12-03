#!/bin/bash

echo "=== Stopping StreamSocial Trending System ==="

# Kill processes
for pidfile in dashboard.pid generator.pid processor.pid; do
    if [ -f "$pidfile" ]; then
        PID=$(cat "$pidfile")
        if ps -p $PID > /dev/null 2>&1; then
            echo "Stopping process $PID..."
            kill $PID
        fi
        rm "$pidfile"
    fi
done

echo "✓ System stopped"
