#!/bin/bash

echo "🔨 Building Day 21: Manual Partition Assignment System"

# Activate virtual environment
source venv/bin/activate

# Install/update dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories
mkdir -p logs state_store

# Set Python path
export PYTHONPATH=$PWD:$PYTHONPATH

echo "✅ Build complete!"
echo "Next steps:"
echo "  ./start.sh     - Start the complete system"
echo "  ./test.sh      - Run tests"
echo "  ./demo.sh      - Run demo"
