#!/bin/bash
set -e

echo "=============================================="
echo "Building StreamSocial ML Training Data Sync"
echo "=============================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtual environment
source venv/bin/activate

# Run unit tests
echo "Running unit tests..."
python -m pytest tests/unit -v --tb=short

echo "Build successful!"
