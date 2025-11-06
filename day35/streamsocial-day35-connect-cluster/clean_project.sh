#!/bin/bash
cd /home/systemdr/git/streamscial/day35/streamsocial-day35-connect-cluster

echo "=========================================="
echo "Cleaning Project"
echo "=========================================="
echo ""

# Remove venv
echo "Removing venv..."
if [ -d "venv" ]; then
    rm -rf venv
    echo "  ✓ venv removed"
else
    echo "  - venv not found"
fi

# Remove node_modules
echo "Removing node_modules..."
if [ -d "node_modules" ]; then
    rm -rf node_modules
    echo "  ✓ node_modules removed"
else
    echo "  - node_modules not found"
fi

# Remove .pytest_cache
echo "Removing .pytest_cache..."
find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null
echo "  ✓ .pytest_cache removed"

# Remove __pycache__
echo "Removing __pycache__ directories..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
echo "  ✓ __pycache__ removed"

# Remove .pyc files
echo "Removing .pyc files..."
find . -type f -name "*.pyc" -delete 2>/dev/null
echo "  ✓ .pyc files removed"

# Remove .pyo files
echo "Removing .pyo files..."
find . -type f -name "*.pyo" -delete 2>/dev/null
echo "  ✓ .pyo files removed"

echo ""
echo "=========================================="
echo "✓ Cleanup complete!"
echo "=========================================="
echo ""
echo "Cleaned:"
echo "  • venv/"
echo "  • node_modules/"
echo "  • .pytest_cache/"
echo "  • __pycache__/"
echo "  • *.pyc files"
echo "  • *.pyo files"
echo ""

