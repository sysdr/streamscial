#!/bin/bash
set -e

echo "🏗️ Building StreamSocial Graceful Shutdown System"

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    python3.11 -m venv venv
fi
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Build completed successfully"
