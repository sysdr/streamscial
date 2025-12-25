#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building Day 56 TLS project..."

# Create virtual environment
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

# Use venv's python with -m pip
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

# Upgrade pip
"$VENV_PYTHON" -m pip install --upgrade pip

# Install dependencies
"$VENV_PYTHON" -m pip install \
    kafka-python==2.0.2 \
    flask==3.0.0 \
    pytest==7.4.3 \
    requests==2.31.0

echo "Build complete!"
echo "Certificate validity: $(openssl x509 -in "$SCRIPT_DIR/certs/broker-cert.pem" -noout -enddate)"
