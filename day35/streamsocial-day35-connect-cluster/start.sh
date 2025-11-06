#!/bin/bash

echo "Starting StreamSocial Connect Cluster..."

# Start Docker services
docker compose up -d

echo "Waiting for services to be ready..."
sleep 30

# Activate virtual environment and start application
source venv/bin/activate
python src/main.py
