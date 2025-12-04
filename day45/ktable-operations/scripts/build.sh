#!/bin/bash
set -e

echo "========================================="
echo "Building Day 45: KTable Reputation System"
echo "========================================="

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Verify installation
echo "Verifying installation..."
python -c "import kafka; import fastapi; import rocksdict; print('All dependencies installed successfully')"

echo "Build complete!"

