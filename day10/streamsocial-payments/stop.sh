#!/bin/bash

echo "🛑 Stopping StreamSocial Payment Processing System"

# Stop Python application
pkill -f "python main.py"

# Stop Docker services
docker-compose down

# Deactivate virtual environment
deactivate 2>/dev/null || true

echo "✅ System stopped successfully"
