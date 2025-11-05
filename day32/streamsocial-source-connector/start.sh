#!/bin/bash

echo "Starting StreamSocial Source Connector..."

# Start with Docker Compose
docker-compose -f docker/docker-compose.yml up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Check if services are running
docker-compose -f docker/docker-compose.yml ps

echo "Services started. Dashboard available at: http://localhost:8080"
echo "Monitoring available at: http://localhost:8082"
