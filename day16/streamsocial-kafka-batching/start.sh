#!/bin/bash

echo "🚀 Starting StreamSocial Kafka Batching System..."

# Activate virtual environment
source venv/bin/activate

# Start background services
cd docker && docker-compose up -d && cd ..

# Wait for services
sleep 10

# Start Flask dashboard
echo "🌐 Starting dashboard at http://localhost:5000"
python dashboard/app.py &

echo "✅ All services started!"
echo "🌐 Dashboard: http://localhost:5000"
echo "📊 Prometheus: http://localhost:9090"
echo "📈 Grafana: http://localhost:3000"

