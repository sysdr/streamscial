#!/bin/bash
set -e

echo "Building Day 46: Stream-Table Joins..."

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

echo "Build complete!"
