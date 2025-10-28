#!/bin/bash
set -e

echo "🧪 StreamSocial Schema Evolution - Test Script"
echo "============================================="

# Activate virtual environment
source venv/bin/activate

# Run unit tests
echo "🔬 Running unit tests..."
python -m pytest tests/test_schema_evolution.py -v

# Run integration tests
echo "🔗 Running integration tests..."
python -m pytest tests/test_integration.py -v

echo ""
echo "✅ All tests completed!"
