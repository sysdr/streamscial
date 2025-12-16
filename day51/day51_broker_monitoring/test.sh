#!/bin/bash

source venv/bin/activate

echo "Running unit tests..."
pytest tests/ -v --tb=short

echo ""
echo "Tests complete!"
