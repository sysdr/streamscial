#!/bin/bash

set -e

echo "=========================================="
echo "Building Day 43: Stateless Transformations"
echo "=========================================="

# Check Python version
if command -v python3.11 &> /dev/null; then
    PYTHON_CMD=python3.11
elif command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
else
    echo "Error: Python 3 not found"
    exit 1
fi

python_version=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
echo "Using Python $python_version"

# Create and activate virtual environment
echo "Creating virtual environment..."
$PYTHON_CMD -m venv venv
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt --quiet

echo "✓ Build complete!"
echo ""
echo "Next steps:"
echo "1. Start Docker services: cd docker && docker-compose up -d"
echo "2. Run tests: pytest tests/ -v"
echo "3. Start processor: python src/stream_processor.py"
echo "4. Start dashboard: python web/dashboard.py"
echo "5. Generate load: python tests/generate_load.py --rate 1000"
