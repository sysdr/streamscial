#!/bin/bash
echo "🛑 Stopping StreamSocial Day 20 System"

# Stop dashboard
if [ -f .dashboard.pid ]; then
    kill $(cat .dashboard.pid) 2>/dev/null
    rm -f .dashboard.pid
fi

# Stop Docker services
echo "🐳 Stopping Docker services..."
cd docker
docker-compose down
cd ..

echo "✅ System stopped"
