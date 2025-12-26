#!/bin/bash
set -e

source venv/bin/activate

echo "Running integration tests..."
python -m pytest tests/test_microservices.py -v -s

echo "✓ All tests passed"
