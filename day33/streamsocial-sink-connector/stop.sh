#!/bin/bash

echo "======================================"
echo "  Stopping StreamSocial Services     "
echo "======================================"

# Stop Docker services
cd docker
docker-compose down

# Kill any remaining processes
pkill -f "python src/main.py" || true
pkill -f "python src/streamsocial/utils/test_data_producer.py" || true

cd ..

echo "All services stopped!"
