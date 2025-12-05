#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "Error: venv directory not found. Please run ./build.sh first."
    exit 1
fi

source venv/bin/activate

echo "Starting Kafka infrastructure..."
docker-compose up -d

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Starting Join Processor..."
python src/processors/join_processor.py &
PROCESSOR_PID=$!

sleep 3

echo "Starting Profile Producer..."
python src/producers/profile_producer.py &
PROFILE_PID=$!

sleep 2

echo "Starting Action Producer..."
python src/producers/action_producer.py &
ACTION_PID=$!

sleep 2

echo "Starting Dashboard..."
python web/dashboard.py &
DASHBOARD_PID=$!

echo ""
echo "========================================="
echo "Stream-Table Joins System Running!"
echo "========================================="
echo "Dashboard: http://localhost:8000"
echo ""
echo "Press Ctrl+C to stop all services"

# Save PIDs
echo $PROCESSOR_PID > .processor.pid
echo $PROFILE_PID > .profile.pid
echo $ACTION_PID > .action.pid
echo $DASHBOARD_PID > .dashboard.pid

wait
