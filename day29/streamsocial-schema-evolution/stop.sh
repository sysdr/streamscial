#!/bin/bash

echo "🛑 Stopping StreamSocial Schema Evolution"
echo "========================================"

# Stop dashboard if running
echo "🎨 Stopping dashboard..."
pkill -f "python src/dashboard/app.py" 2>/dev/null || true

# Stop Docker services
echo "🐳 Stopping Docker services..."
cd docker
docker compose down
cd ..

echo "✅ All services stopped"
