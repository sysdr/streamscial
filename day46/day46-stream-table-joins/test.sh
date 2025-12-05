#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "Error: venv directory not found. Please run ./build.sh first."
    exit 1
fi

source venv/bin/activate

echo "Running tests..."
python -m pytest tests/ -v

echo ""
echo "All tests passed!"
