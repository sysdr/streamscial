#!/bin/bash
echo "🛑 Stopping StreamSocial..."

if [ -f .pids ]; then
    while read -r line; do
        if [[ $line == Backend* ]]; then
            PID=$(echo $line | cut -d' ' -f3)
            kill $PID 2>/dev/null
            echo "Stopped backend (PID: $PID)"
        fi
    done < .pids
    rm .pids
fi

# Kill any remaining uvicorn processes
pkill -f uvicorn 2>/dev/null

echo "✅ StreamSocial stopped"
