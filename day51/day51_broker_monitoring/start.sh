#!/bin/bash

echo "Starting StreamSocial Broker Monitoring System..."

# Start Kafka cluster
docker-compose up -d

echo "Waiting for Kafka cluster to be ready..."
sleep 30

# Start dashboard
source venv/bin/activate
python dashboards/app.py &

DASHBOARD_PID=$!

echo ""
echo "System started!"
echo "Dashboard: http://localhost:5000"
echo "Dashboard PID: $DASHBOARD_PID"
echo ""
echo "To stop: ./stop.sh"
