#!/bin/bash

echo "Stopping StreamSocial Broker Monitoring System..."

# Stop dashboard
pkill -f "python dashboards/app.py"

# Stop Kafka
docker-compose down

echo "System stopped!"
