#!/bin/bash

echo "🛑 Stopping StreamSocial Day 8 Demo"

# Stop Docker services
cd docker
docker-compose down -v
cd ..

# Deactivate virtual environment
deactivate

echo "✅ All services stopped"
