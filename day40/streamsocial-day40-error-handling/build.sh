#!/bin/bash
set -e

echo "=========================================="
echo "Building StreamSocial Day 40"
echo "=========================================="

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "✓ Build completed successfully"
