#!/bin/bash

echo "🚀 Starting StreamSocial Async Producer..."

# Activate virtual environment
source venv/bin/activate

# Start Kafka if not running
if ! docker ps | grep -q kafka; then
    echo "🐳 Starting Kafka..."
    cd docker
    docker-compose up -d
    cd ..
    sleep 15
fi

# Start the application
echo "🌐 Starting web server on http://localhost:5000"
python src/api_server.py &
APP_PID=$!

echo "✅ Application started!"
echo ""
echo "🌐 Dashboard: http://localhost:5000/dashboard"
echo "📊 Metrics:   http://localhost:5000/metrics"
echo "🩺 Health:    http://localhost:5000/health"
echo ""
echo "Press Ctrl+C to stop"

# Wait for interrupt
trap "kill $APP_PID 2>/dev/null; exit" INT
wait $APP_PID
