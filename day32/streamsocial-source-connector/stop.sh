#!/bin/bash

echo "Stopping StreamSocial Source Connector..."

# Stop Docker Compose services
docker-compose -f docker/docker-compose.yml down

echo "Services stopped."
