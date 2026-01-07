#!/bin/bash
set -e

echo "Building StreamSocial Production System..."

# Activate virtual environment
source venv/bin/activate

# Run tests
echo "Running test suite..."
python -m pytest tests/ -v --tb=short

echo "✓ Build completed successfully"
echo "✓ All tests passed"
echo ""
echo "Production system ready for deployment"
