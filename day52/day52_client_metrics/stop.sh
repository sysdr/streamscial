#!/bin/bash

echo "Stopping Day 52 services..."
pkill -f "python src/dashboard/app.py" || true
docker-compose down
echo "Services stopped."
