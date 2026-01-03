#!/bin/bash

echo "=========================================="
echo "Running Schema Governance Demo"
echo "=========================================="

# Activate virtual environment
source venv/bin/activate

# Run demo
python src/main_demo.py

echo ""
echo "=========================================="
echo "Demo Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. View dashboard: http://localhost:5058"
echo "  2. Run tests: pytest tests/ -v"
echo "  3. Experiment with different compatibility modes"
