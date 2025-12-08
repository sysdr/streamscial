#!/bin/bash

echo "Running tests..."

source venv/bin/activate

pytest tests/ -v --tb=short

echo "Tests complete!"
