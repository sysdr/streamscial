#!/bin/bash

echo "⏹️ Stopping StreamSocial Headers System..."

# Stop Docker services
docker-compose -f docker/docker-compose.yml down

echo "✅ All services stopped!"
