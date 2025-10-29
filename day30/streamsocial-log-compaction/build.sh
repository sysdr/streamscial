#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🏗️  Building StreamSocial Log Compaction Demo..."

# Check if requirements.txt exists
if [ ! -f "$SCRIPT_DIR/requirements.txt" ]; then
    echo "❌ Error: requirements.txt not found in $SCRIPT_DIR"
    exit 1
fi

# Check Python version
echo "🐍 Checking Python version..."
if ! python3.11 --version > /dev/null 2>&1; then
    echo "❌ Python 3.11 not found. Please install Python 3.11"
    exit 1
fi

# Create and activate virtual environment
echo "🌍 Creating virtual environment..."
VENV_PATH="$SCRIPT_DIR/venv"
if [ -d "$VENV_PATH" ]; then
    echo "⚠️  Virtual environment already exists, removing old one..."
    rm -rf "$VENV_PATH"
fi
python3.11 -m venv "$VENV_PATH"

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing dependencies from $SCRIPT_DIR/requirements.txt..."
pip install -r "$SCRIPT_DIR/requirements.txt"

echo "✅ Build completed successfully!"
echo "📋 Next steps:"
echo "   1. Start Kafka: docker-compose up -d"
echo "   2. Run tests: ./test.sh"
echo "   3. Start demo: ./start.sh"
