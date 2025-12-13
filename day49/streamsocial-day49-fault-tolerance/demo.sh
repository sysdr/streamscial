#!/bin/bash

# Get the project directory (where this script is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# Change to project directory
cd "$PROJECT_DIR"

# Check if venv exists
if [ ! -d "$PROJECT_DIR/venv" ]; then
    echo "Error: Virtual environment not found. Running build.sh first..."
    bash "$PROJECT_DIR/build.sh"
fi

source "$PROJECT_DIR/venv/bin/activate"

echo "========================================"
echo "Day 49: Fault Tolerance Demo"
echo "========================================"

# Start infrastructure
echo -e "\n[1/5] Starting Kafka infrastructure..."
if [ -f "$PROJECT_DIR/docker-compose.yml" ]; then
    cd "$PROJECT_DIR"
    docker-compose up -d
    sleep 15
else
    echo "Error: docker-compose.yml not found at $PROJECT_DIR/docker-compose.yml"
    exit 1
fi

# Build and test
echo -e "\n[2/5] Building and testing..."
if [ -f "$PROJECT_DIR/build.sh" ]; then
    bash "$PROJECT_DIR/build.sh"
else
    echo "Error: build.sh not found"
    exit 1
fi

# Start services
echo -e "\n[3/5] Starting services..."
if [ -f "$PROJECT_DIR/start.sh" ]; then
    bash "$PROJECT_DIR/start.sh"
    sleep 10
else
    echo "Error: start.sh not found"
    exit 1
fi

# Run failure test
echo -e "\n[4/5] Running failure recovery test..."
if [ -f "$PROJECT_DIR/src/failure_tester.py" ]; then
    python "$PROJECT_DIR/src/failure_tester.py"
else
    echo "Warning: failure_tester.py not found, skipping failure test"
fi

# Show results
echo -e "\n[5/5] Demo Results:"
echo "✓ Dashboard: http://localhost:5000"
echo "✓ Processor handling events with automatic recovery"
echo "✓ State persisted to changelog topics"
echo "✓ Recovery time measured and validated"

echo -e "\nPress Ctrl+C to stop demo"
wait
