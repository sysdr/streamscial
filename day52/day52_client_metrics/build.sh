#!/bin/bash
set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "Building Day 52 Project..."

# Activate virtual environment
if [ ! -f "${SCRIPT_DIR}/venv/bin/activate" ]; then
    echo "Error: Virtual environment not found at ${SCRIPT_DIR}/venv"
    exit 1
fi
source "${SCRIPT_DIR}/venv/bin/activate"

# Run tests
echo "Running tests..."
pytest "${SCRIPT_DIR}/tests/" -v

echo "Build complete!"
