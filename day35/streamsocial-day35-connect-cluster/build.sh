#!/bin/bash

echo "Building StreamSocial Connect Cluster..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Pull Docker images (if needed)
echo "Pulling Docker images..."
docker compose pull

echo "Build complete!"
echo "You can now run ./start.sh to start the cluster"

