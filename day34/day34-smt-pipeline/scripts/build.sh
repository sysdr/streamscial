#!/bin/bash
set -e

echo "🔨 Building Day 34 SMT Pipeline..."

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment with Python 3.11..."
    python3.11 -m venv venv
fi

source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Build complete!"
