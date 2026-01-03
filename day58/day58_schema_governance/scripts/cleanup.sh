#!/bin/bash

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Change to project root
cd "$PROJECT_ROOT"

echo "=========================================="
echo "Docker Cleanup Script"
echo "=========================================="

# Stop all running containers
echo "Stopping all running containers..."
docker stop $(docker ps -q) 2>/dev/null || echo "No running containers to stop"

# Remove all stopped containers
echo "Removing all stopped containers..."
docker rm $(docker ps -a -q) 2>/dev/null || echo "No containers to remove"

# Remove unused images
echo "Removing unused Docker images..."
docker image prune -a -f

# Remove unused volumes
echo "Removing unused Docker volumes..."
docker volume prune -f

# Remove unused networks
echo "Removing unused Docker networks..."
docker network prune -f

# Remove all unused Docker resources (containers, networks, images, build cache)
echo "Removing all unused Docker resources..."
docker system prune -a -f --volumes

echo ""
echo "=========================================="
echo "✓ Docker cleanup completed!"
echo "=========================================="

