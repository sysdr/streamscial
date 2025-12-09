#!/bin/bash

echo "Running tests..."
source venv/bin/activate

# Ensure system is running
if ! curl -s http://localhost:8080/api/health > /dev/null; then
    echo "Error: System not running. Start with './start.sh' first"
    exit 1
fi

pytest tests/ -v --tb=short
