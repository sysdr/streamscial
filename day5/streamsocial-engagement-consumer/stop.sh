#!/bin/bash

echo "🛑 Stopping StreamSocial Services..."

# Kill Python processes
pkill -f "dashboard.py"
pkill -f "demo_producer.py"  
pkill -f "engagement_consumer.py"

# Stop Docker services
cd docker && docker-compose down

echo "✅ All services stopped"
