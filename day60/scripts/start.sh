#!/bin/bash
set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Starting StreamSocial Production Dashboard..."

# Check if virtual environment exists
VENV_PATH="$PROJECT_ROOT/venv"
if [ ! -d "$VENV_PATH" ]; then
    echo "Error: Virtual environment not found at $VENV_PATH"
    exit 1
fi

# Check if dashboard script exists
DASHBOARD_SCRIPT="$PROJECT_ROOT/src/monitoring/production_dashboard.py"
if [ ! -f "$DASHBOARD_SCRIPT" ]; then
    echo "Error: Dashboard script not found at $DASHBOARD_SCRIPT"
    exit 1
fi

# Check if logs directory exists
LOGS_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOGS_DIR"

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Check if port 8000 is already in use
if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo "Warning: Port 8000 is already in use. Checking for existing dashboard..."
    EXISTING_PID=$(lsof -Pi :8000 -sTCP:LISTEN -t)
    echo "Found process with PID: $EXISTING_PID"
    read -p "Kill existing process and start new one? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kill $EXISTING_PID 2>/dev/null || true
        sleep 2
    else
        echo "Aborted. Please stop the existing service first."
        exit 1
    fi
fi

# Change to project root
cd "$PROJECT_ROOT"

# Start dashboard in background
python "$DASHBOARD_SCRIPT" &
DASHBOARD_PID=$!

# Wait a moment for the server to start
sleep 3

# Check if process is still running
if ! kill -0 $DASHBOARD_PID 2>/dev/null; then
    echo "Error: Dashboard failed to start"
    exit 1
fi

echo "✓ Dashboard started (PID: $DASHBOARD_PID)"
echo ""
echo "Dashboard available at: http://localhost:8000"
echo "Press Ctrl+C to stop"

# Store PID for stop script
echo $DASHBOARD_PID > "$LOGS_DIR/dashboard.pid"

# Wait for interrupt
wait $DASHBOARD_PID
