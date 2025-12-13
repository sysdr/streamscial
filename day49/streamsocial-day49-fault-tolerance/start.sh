#!/bin/bash

# Get the project directory (where this script is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
VENV_DIR="$PROJECT_DIR/venv"

# Change to project directory
cd "$PROJECT_DIR"

# Check if venv exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Error: Virtual environment not found at $VENV_DIR"
    echo "Please run ./build.sh first"
    exit 1
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Check for duplicate services
if pgrep -f "python.*dashboard.py" > /dev/null; then
    echo "Warning: Dashboard is already running. Stopping existing instance..."
    pkill -f "python.*dashboard.py"
    sleep 2
fi

if pgrep -f "python.*engagement_processor.py" > /dev/null; then
    echo "Warning: Processor is already running. Stopping existing instance..."
    pkill -f "python.*engagement_processor.py"
    sleep 2
fi

echo "Starting StreamSocial Fault Tolerance Demo..."

# Check if files exist
if [ ! -f "$PROJECT_DIR/src/dashboard.py" ]; then
    echo "Error: dashboard.py not found at $PROJECT_DIR/src/dashboard.py"
    exit 1
fi

if [ ! -f "$PROJECT_DIR/src/engagement_processor.py" ]; then
    echo "Error: engagement_processor.py not found at $PROJECT_DIR/src/engagement_processor.py"
    exit 1
fi

# Start dashboard
python "$PROJECT_DIR/src/dashboard.py" &
DASHBOARD_PID=$!
echo "Dashboard started (PID: $DASHBOARD_PID) - http://localhost:5000"

# Wait for dashboard
sleep 3

# Start processor
python "$PROJECT_DIR/src/engagement_processor.py" &
PROCESSOR_PID=$!
echo "Processor started (PID: $PROCESSOR_PID)"

# Wait for processor
sleep 5

# Generate events
echo "Generating events..."
if [ -f "$PROJECT_DIR/src/event_generator.py" ]; then
    python "$PROJECT_DIR/src/event_generator.py" &
else
    echo "Warning: event_generator.py not found, skipping event generation"
fi

echo -e "\n✓ System running!"
echo "Dashboard: http://localhost:5000"
echo "Stop with: ./stop.sh"

# Save PIDs
echo $DASHBOARD_PID > /tmp/dashboard.pid
echo $PROCESSOR_PID > /tmp/processor.pid
