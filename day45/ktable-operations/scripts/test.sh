#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

if [ ! -f "$PROJECT_DIR/venv/bin/activate" ]; then
    echo "Error: Virtual environment not found. Run ./scripts/build.sh first"
    exit 1
fi

source "$PROJECT_DIR/venv/bin/activate"

echo "Running tests..."
pytest "$PROJECT_DIR/tests/" -v --tb=short

echo "All tests passed!"

