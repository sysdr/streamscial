#!/bin/bash

echo "⏹️ Stopping StreamSocial Day 27 Services"

# Stop Docker services
cd docker && docker-compose down

# Kill any Python processes
pkill -f "python.*dashboard" || true
pkill -f "python.*demo" || true

echo "✅ All services stopped!"
