#!/bin/bash

echo "🧪 Running tests for StreamSocial Day 27"

# Activate virtual environment
source venv_day27/bin/activate

# Run unit tests
echo "📋 Running unit tests..."
python -m pytest tests/ -v --tb=short

# Run integration tests
echo "📋 Running integration tests..."
python scripts/demo.py

echo "✅ All tests completed!"
