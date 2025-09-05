#!/bin/bash

# StreamSocial Timeline Ordering Build Script
set -e

echo "🏗️  Building StreamSocial Timeline Ordering System"
echo "================================================="

# Check Python version
echo "🐍 Checking Python version..."
python3.11 --version || {
    echo "❌ Python 3.11 is required but not found"
    exit 1
}

# Create and activate virtual environment
echo "📦 Setting up virtual environment..."
if [ ! -d "venv" ]; then
    python3.11 -m venv venv
fi
source venv/bin/activate

# Verify Python version in venv
echo "✅ Python version in virtual environment:"
python --version

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

echo "✅ Build completed successfully!"

# Verify installation
echo "🔍 Verifying installation..."
python -c "
import kafka
import fastapi
import pandas
import structlog
print('✅ All major dependencies imported successfully')
"

# Create topic in Kafka (will be created automatically, but good to verify)
echo "📝 Build summary:"
echo "  - Virtual environment: $(which python)"
echo "  - Python version: $(python --version)"
echo "  - Dependencies: $(pip list | wc -l) packages installed"
echo ""
echo "🚀 Ready for testing! Run './test.sh' to execute tests."
echo "🌐 Ready for demo! Run './start.sh' to start the application."
