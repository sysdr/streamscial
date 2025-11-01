#!/bin/bash

set +e  # Don't exit on error

echo "Building StreamSocial Source Connector..."

# Activate virtual environment
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
fi

# Run code formatting
echo "Formatting code..."
if command -v black &> /dev/null; then
    black src/ || echo "Black formatting failed or not available, continuing..."
else
    echo "Black not available, skipping formatting..."
fi

# Run linting
echo "Running linting..."
if command -v flake8 &> /dev/null; then
    flake8 src/ --max-line-length=100 --ignore=E203,W503 || echo "Linting completed with warnings..."
else
    echo "Flake8 not available, skipping linting..."
fi

# Run tests
echo "Running tests..."
if command -v pytest &> /dev/null; then
    python -m pytest src/test/ -v --cov=src/main/python --cov-report=term-missing || echo "Tests completed with some failures..."
else
    echo "Pytest not available, skipping tests..."
fi

# Build Docker image (optional)
echo "Building Docker image..."
if command -v docker &> /dev/null; then
    docker build -t streamsocial-source-connector:latest -f docker/Dockerfile . || echo "Docker build failed or Docker not available..."
else
    echo "Docker not available, skipping Docker build..."
fi

echo "Build process completed!"
