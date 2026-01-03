#!/bin/bash

echo "=========================================="
echo "Building Schema Governance Framework"
echo "=========================================="

# Create and activate virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Install pytest
pip install pytest

echo "✓ Build complete!"
echo ""
echo "Virtual environment activated. Dependencies installed."
