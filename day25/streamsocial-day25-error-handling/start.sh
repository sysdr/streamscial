#!/bin/bash

echo "🚀 Starting StreamSocial Error Handling System..."

# Activate virtual environment
source venv/bin/activate

# Ensure infrastructure is running
docker-compose up -d

# Wait for services
sleep 10

# Start the application
echo "🎯 Starting StreamSocial application..."
python src/main.py
