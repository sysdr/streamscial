#!/bin/bash

set -e

echo "======================================"
echo "   StreamSocial Sink Connector Build  "
echo "======================================"

# Activate virtual environment
source venv/bin/activate

# Create necessary directories
mkdir -p logs data/input data/output

# Install dependencies
echo "Installing/updating dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Run tests
echo "Running unit tests..."
python -m pytest tests/unit/ -v

echo "Running integration tests..."
python -m pytest tests/integration/ -v --disable-warnings

# Verify source files
echo "Verifying source files..."
python -m py_compile src/streamsocial/connectors/hashtag_sink_connector.py
python -m py_compile src/streamsocial/utils/database_manager.py
python -m py_compile src/streamsocial/models/hashtag_model.py
python -m py_compile src/web/app.py
python -m py_compile src/main.py

echo "Build completed successfully!"
echo ""
echo "Next steps:"
echo "1. Start dependencies: ./start.sh"
echo "2. Run tests: ./test.sh"
echo "3. Start demo: ./demo.sh"
