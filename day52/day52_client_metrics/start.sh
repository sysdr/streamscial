#!/bin/bash
set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "Starting Day 52: Client Metrics Dashboard..."

# Check if Kafka is running
if ! nc -z localhost 9092 2>/dev/null; then
    echo "Starting Kafka with Docker Compose..."
    docker-compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d
    echo "Waiting for Kafka to be ready..."
    sleep 15
fi

# Activate virtual environment
if [ ! -f "${SCRIPT_DIR}/venv/bin/activate" ]; then
    echo "Error: Virtual environment not found at ${SCRIPT_DIR}/venv"
    exit 1
fi
source "${SCRIPT_DIR}/venv/bin/activate"

# Create topics
echo "Creating Kafka topics..."
python "${SCRIPT_DIR}/src/producers/instrumented_producer.py"

# Start dashboard
echo "Starting dashboard on http://localhost:5052"
python "${SCRIPT_DIR}/src/dashboard/app.py"
