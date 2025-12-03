#!/bin/bash
set -e

echo "=== Building StreamSocial Trending System ==="

# Create and activate virtual environment
echo "[1/5] Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
echo "[2/5] Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "[3/5] Installing dependencies..."
pip install kafka-python==2.0.2 \
            pyyaml==6.0.1 \
            flask==3.0.0 \
            pytest==7.4.3 \
            pytest-cov==4.1.0

# Run tests
echo "[4/5] Running tests..."
PYTHONPATH=. pytest tests/ -v --cov=src --cov-report=term-missing

echo "[5/5] Build complete!"
echo ""
echo "Next steps:"
echo "  1. Start Kafka: docker-compose up -d"
echo "  2. Run processor: ./start.sh"
echo "  3. View dashboard: http://localhost:5000"
