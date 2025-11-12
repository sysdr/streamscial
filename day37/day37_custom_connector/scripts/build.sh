#!/bin/bash

# Build script - sets up environment and runs tests

set -e

echo "Building Day 37: Custom Connector Development"
echo "=============================================="

# Activate virtual environment
source venv/bin/activate

# Run tests
echo ""
echo "Running tests..."
python -m pytest tests/ -v

echo ""
echo "Build successful!"
echo ""
echo "Next steps:"
echo "  1. Run 'bash scripts/start.sh' to start connector"
echo "  2. Open http://localhost:5000 for dashboard"
echo "  3. Open http://localhost:8080/metrics for Prometheus metrics"
