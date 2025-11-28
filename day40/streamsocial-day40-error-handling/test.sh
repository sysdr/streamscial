#!/bin/bash
set -e

echo "=========================================="
echo "Running Tests"
echo "=========================================="

source venv/bin/activate

echo "Running unit tests..."
pytest tests/ -v

echo ""
echo "✓ All tests passed"
