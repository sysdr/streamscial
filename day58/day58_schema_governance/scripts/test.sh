#!/bin/bash

echo "=========================================="
echo "Testing Schema Governance Framework"
echo "=========================================="

# Activate virtual environment
source venv/bin/activate

# Run tests
pytest tests/test_schema_governance.py -v --tb=short

echo ""
echo "=========================================="
echo "Tests Complete!"
echo "=========================================="
