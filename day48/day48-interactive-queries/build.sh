#!/bin/bash

echo "======================================"
echo "Building Day 48: Interactive Queries"
echo "======================================"

# Create and activate virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories
mkdir -p data/state-stores
mkdir -p logs

echo ""
echo "Build complete!"
echo "Run './start.sh' to start the system"
