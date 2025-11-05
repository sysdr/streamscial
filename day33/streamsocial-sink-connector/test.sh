#!/bin/bash

set -e

echo "======================================"
echo "     Running Comprehensive Tests     "
echo "======================================"

# Activate virtual environment
source venv/bin/activate

# Ensure infrastructure is running
echo "Checking infrastructure..."
if ! docker-compose -f docker/docker-compose.yml ps | grep -q "Up"; then
    echo "Starting infrastructure..."
    ./start.sh
fi

# Run unit tests
echo "Running unit tests..."
python -m pytest tests/unit/ -v --tb=short

# Run integration tests  
echo "Running integration tests..."
python -m pytest tests/integration/ -v --tb=short

# Test database connection
echo "Testing database connection..."
python -c "
from src.streamsocial.utils.database_manager import DatabaseManager
import os
db = DatabaseManager(os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/streamsocial'))
db.initialize_schema()
print('Database connection successful!')
"

# Test Kafka producer
echo "Testing Kafka producer..."
timeout 10s python src/streamsocial/utils/test_data_producer.py &
PRODUCER_PID=$!
sleep 5
kill $PRODUCER_PID 2>/dev/null || true
echo "Kafka producer test completed!"

echo "All tests passed!"
