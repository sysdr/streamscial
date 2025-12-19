#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=================================================="
echo "Building Day 53: Centralized Logging System"
echo "=================================================="

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "Error: venv directory not found in $SCRIPT_DIR"
    exit 1
fi

# Check if tests directory exists
if [ ! -d "tests" ]; then
    echo "Error: tests directory not found in $SCRIPT_DIR"
    exit 1
fi

# Activate virtual environment
source "$SCRIPT_DIR/venv/bin/activate"

# Run tests
echo ""
echo "Running tests..."
python -m pytest "$SCRIPT_DIR/tests/" -v

echo ""
echo "Build completed successfully!"
echo ""
echo "Next steps:"
echo "1. Start infrastructure: ./start.sh"
echo "2. Run demo: ./demo.sh"
echo "3. Stop infrastructure: ./stop.sh"
