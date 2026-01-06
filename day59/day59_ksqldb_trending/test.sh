#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "=== Testing Day 59: ksqlDB Trending Analytics ==="

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "ERROR: Virtual environment not found. Please run setup.sh first."
    exit 1
fi

source venv/bin/activate

# Run pytest
pytest tests/test_ksqldb_system.py -v --tb=short

echo "✓ Tests complete!"
