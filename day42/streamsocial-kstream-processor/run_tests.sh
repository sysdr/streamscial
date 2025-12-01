#!/bin/bash
set -e

source venv/bin/activate

echo "=== Running Unit Tests ==="
python -m pytest tests/test_models.py -v
python -m pytest tests/test_scoring.py -v
python -m pytest tests/test_validator.py -v

echo ""
echo "=== Test Summary ==="
python -m pytest tests/ -v --tb=short

echo ""
echo "All tests passed!"
