#!/bin/bash
##############################################################################
# Cleanup Script for StreamSocial Day 60 Project
# Stops all services, Docker containers, and removes unused resources
##############################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "StreamSocial Day 60 - Cleanup Script"
echo "=============================================="
echo ""

# Step 1: Stop all Python services
echo "[1/6] Stopping Python services..."
pkill -f production_dashboard 2>/dev/null || echo "  No production dashboard running"
pkill -f uvicorn 2>/dev/null || echo "  No uvicorn services running"
pkill -f "python.*src/monitoring" 2>/dev/null || echo "  No monitoring services running"
sleep 2
echo "  ✓ Python services stopped"
echo ""

# Step 2: Stop and remove Docker containers
echo "[2/6] Stopping and removing Docker containers..."
if command -v docker >/dev/null 2>&1; then
    # Stop all running containers
    RUNNING_CONTAINERS=$(docker ps -q 2>/dev/null || true)
    if [ -n "$RUNNING_CONTAINERS" ]; then
        echo "  Stopping running containers..."
        docker stop $RUNNING_CONTAINERS 2>/dev/null || true
        echo "  ✓ Stopped running containers"
    else
        echo "  No running containers found"
    fi
    
    # Remove all containers
    ALL_CONTAINERS=$(docker ps -a -q 2>/dev/null || true)
    if [ -n "$ALL_CONTAINERS" ]; then
        echo "  Removing containers..."
        docker rm $ALL_CONTAINERS 2>/dev/null || true
        echo "  ✓ Removed containers"
    else
        echo "  No containers to remove"
    fi
else
    echo "  Docker not installed, skipping Docker cleanup"
fi
echo ""

# Step 3: Remove unused Docker resources
echo "[3/6] Cleaning up unused Docker resources..."
if command -v docker >/dev/null 2>&1; then
    # Remove unused images
    echo "  Removing unused images..."
    docker image prune -af 2>/dev/null || true
    
    # Remove unused volumes
    echo "  Removing unused volumes..."
    docker volume prune -af 2>/dev/null || true
    
    # Remove unused networks
    echo "  Removing unused networks..."
    docker network prune -af 2>/dev/null || true
    
    # System prune (optional - removes everything unused)
    echo "  Running system prune..."
    docker system prune -af --volumes 2>/dev/null || true
    
    echo "  ✓ Docker cleanup completed"
else
    echo "  Docker not installed, skipping"
fi
echo ""

# Step 4: Remove Python cache and build artifacts
echo "[4/6] Removing Python cache and build artifacts..."
# Remove __pycache__ directories
find . -type d -name "__pycache__" ! -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
echo "  ✓ Removed __pycache__ directories"

# Remove .pyc files
find . -type f -name "*.pyc" ! -path "./.git/*" -delete 2>/dev/null || true
echo "  ✓ Removed .pyc files"

# Remove .pyo files
find . -type f -name "*.pyo" ! -path "./.git/*" -delete 2>/dev/null || true
echo "  ✓ Removed .pyo files"

# Remove .pytest_cache
if [ -d ".pytest_cache" ]; then
    rm -rf .pytest_cache
    echo "  ✓ Removed .pytest_cache"
fi

# Remove Python build directories
find . -type d -name "*.egg-info" ! -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "build" ! -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "dist" ! -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
echo "  ✓ Removed build artifacts"
echo ""

# Step 5: Remove virtual environment and node_modules
echo "[5/6] Removing virtual environments and node_modules..."
if [ -d "venv" ]; then
    rm -rf venv
    echo "  ✓ Removed venv directory"
else
    echo "  No venv directory found"
fi

if [ -d "node_modules" ]; then
    rm -rf node_modules
    echo "  ✓ Removed node_modules directory"
else
    echo "  No node_modules directory found"
fi
echo ""

# Step 6: Remove Istio files
echo "[6/6] Removing Istio-related files..."
ISTIO_FILES=$(find . -type f -name "*istio*" ! -path "./.git/*" 2>/dev/null || true)
ISTIO_DIRS=$(find . -type d -name "*istio*" ! -path "./.git/*" 2>/dev/null || true)

if [ -n "$ISTIO_FILES" ] || [ -n "$ISTIO_DIRS" ]; then
    find . -type f -name "*istio*" ! -path "./.git/*" -delete 2>/dev/null || true
    find . -type d -name "*istio*" ! -path "./.git/*" -exec rm -rf {} + 2>/dev/null || true
    echo "  ✓ Removed Istio files and directories"
else
    echo "  No Istio files found"
fi
echo ""

echo "=============================================="
echo "✓ Cleanup completed successfully!"
echo "=============================================="
echo ""
echo "Removed:"
echo "  - Python services (stopped)"
echo "  - Docker containers and unused resources"
echo "  - Python cache files (__pycache__, .pyc, .pyo)"
echo "  - .pytest_cache directory"
echo "  - venv directory"
echo "  - node_modules directory"
echo "  - Istio files"
echo "  - Build artifacts"
echo ""
echo "Project is now clean and ready for fresh setup."

