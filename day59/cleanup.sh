#!/bin/bash

# Cleanup script for Day 59: ksqlDB Trending Analytics
# This script stops containers and removes unused Docker resources

set -e

echo "=== Day 59: Docker and Project Cleanup ==="
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Stop Python services if running
echo "1. Stopping Python services..."
pkill -f "dashboard.py" 2>/dev/null || true
pkill -f "event_producer.py" 2>/dev/null || true
sleep 2
echo "   ✓ Python services stopped"

# Stop project containers
echo "2. Stopping project containers..."
if [ -f "day59_ksqldb_trending/stop.sh" ]; then
    cd day59_ksqldb_trending
    bash stop.sh 2>/dev/null || true
    cd ..
fi

# Stop Docker Compose services
if [ -f "day59_ksqldb_trending/docker/docker-compose.yml" ]; then
    cd day59_ksqldb_trending/docker
    docker-compose down 2>/dev/null || true
    cd ../..
fi
echo "   ✓ Project containers stopped"

# Stop and remove day59 containers
echo "3. Removing day59 containers..."
docker ps -a | grep -E "day59" | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true
echo "   ✓ Day59 containers removed"

# Remove day59 networks
echo "4. Removing day59 networks..."
docker network ls | grep -E "day59" | awk '{print $1}' | xargs -r docker network rm 2>/dev/null || true
echo "   ✓ Day59 networks removed"

# Remove unused Docker resources
echo "5. Cleaning up unused Docker resources..."
docker container prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true
echo "   ✓ Unused Docker resources cleaned"

# Remove unused images (optional - commented out to avoid removing images in use)
# echo "6. Removing unused Docker images..."
# docker image prune -a -f 2>/dev/null || true
# echo "   ✓ Unused Docker images removed"

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "Remaining Docker resources:"
docker ps -a 2>/dev/null | head -5 || echo "  (No containers)"
echo ""
docker images 2>/dev/null | head -5 || echo "  (No images)"

