#!/bin/bash
# Build script for StreamSocial Schema System

set -e

echo "🏗️ Building StreamSocial Schema System..."

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Run linting
echo "🔍 Running code checks..."
python -m py_compile src/registry/schema_registry.py
python -m py_compile src/registry/schema_server.py
python -m py_compile src/validators/interceptors/producer_interceptor.py

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

echo "✅ Build completed successfully!"
