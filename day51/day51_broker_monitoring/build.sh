#!/bin/bash

echo "Building StreamSocial Broker Monitoring System..."

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv --without-pip
    venv/bin/python3 -m ensurepip --upgrade
fi

# Ensure pip is available in venv
if [ ! -f "venv/bin/pip3" ]; then
    echo "Installing pip in virtual environment..."
    venv/bin/python3 -m ensurepip --upgrade
fi

# Install dependencies
echo "Installing dependencies..."
venv/bin/pip3 install --upgrade pip
venv/bin/pip3 install -r requirements.txt

echo "Build complete!"
echo ""
echo "Next steps:"
echo "1. Start Kafka cluster: docker-compose up -d"
echo "2. Wait 30 seconds for cluster to be ready"
echo "3. Run tests: ./test.sh"
echo "4. Start dashboard: ./demo.sh"
