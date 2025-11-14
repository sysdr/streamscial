#!/bin/bash
set -e

echo "=========================================="
echo "Running CDC Tests"
echo "=========================================="

# Activate virtual environment
source venv/bin/activate

# Wait for services
echo "Waiting for services..."
sleep 5

# Run tests
echo "Running pytest..."
pytest tests/ -v --tb=short

echo ""
echo "✓ All tests passed!"
