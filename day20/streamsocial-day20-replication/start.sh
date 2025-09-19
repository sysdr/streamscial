#!/bin/bash
echo "🚀 Starting StreamSocial Day 20 System"

# Activate virtual environment
source venv/bin/activate

# Start Docker services
echo "🐳 Starting Docker services..."
cd docker
docker-compose up -d
cd ..

# Wait for services
echo "⏳ Waiting for services to be ready..."
sleep 20

# Start monitoring dashboard
echo "📊 Starting monitoring dashboard..."
python dashboard/app.py &
DASHBOARD_PID=$!

echo "✅ System started successfully!"
echo "🔗 Dashboard: http://localhost:5000"
echo "🛑 To stop: ./stop.sh"

# Save PID for stopping
echo $DASHBOARD_PID > .dashboard.pid
