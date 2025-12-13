#!/bin/bash

# Get the project directory (where this script is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
VENV_DIR="$PROJECT_DIR/venv"

# Change to project directory
cd "$PROJECT_DIR"

echo "Building Day 49: Fault Tolerance & Recovery"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# Install dependencies
pip install --upgrade pip
if [ -f "$PROJECT_DIR/requirements.txt" ]; then
    pip install -r "$PROJECT_DIR/requirements.txt"
else
    echo "Error: requirements.txt not found"
    exit 1
fi

# Run tests
echo -e "\n=== Running Tests ==="
if [ -d "$PROJECT_DIR/tests" ]; then
    python -m pytest "$PROJECT_DIR/tests/" -v
else
    echo "Warning: tests directory not found, skipping tests"
fi

echo -e "\n✓ Build completed successfully"
echo "Next steps:"
echo "  1. Start infrastructure: docker-compose up -d"
echo "  2. Start processor: ./start.sh"
echo "  3. View dashboard: http://localhost:5000"
