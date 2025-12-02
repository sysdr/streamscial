#!/bin/bash

echo "Building Day 41: Kafka Streams Fundamentals..."

# Get absolute path to current directory
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$SCRIPT_DIR"

# Create virtual environment if not exists
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    python3 -m venv "$SCRIPT_DIR/venv"
fi

# Activate virtual environment
source "$SCRIPT_DIR/venv/bin/activate"

# Install dependencies
pip install --upgrade pip
pip install -r "$SCRIPT_DIR/requirements.txt"

# Create necessary directories
mkdir -p /tmp/kafka-streams-state

# Run tests
echo "Running tests..."
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"
pytest "$SCRIPT_DIR/tests/" -v

echo "Build complete!"
