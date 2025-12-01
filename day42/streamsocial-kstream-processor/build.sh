#!/bin/bash
set -e

echo "=== Building StreamSocial KStream Processor ==="

# Activate virtual environment
source venv/bin/activate

# Run tests
echo "Running unit tests..."
python -m pytest tests/ -v

echo "Build complete!"
