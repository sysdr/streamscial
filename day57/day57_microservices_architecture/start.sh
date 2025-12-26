#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting microservices infrastructure..."

# Check for duplicate services
check_port() {
    local port=$1
    local service=$2
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "Warning: Port $port is already in use. $service may already be running."
        return 1
    fi
    return 0
}

check_port 8001 "User Service"
check_port 8002 "Content Service"
check_port 8003 "Notification Service"
check_port 8000 "Dashboard"

# Start Docker services
cd "$SCRIPT_DIR/docker"
docker-compose up -d
cd "$SCRIPT_DIR"

echo "Waiting for services to be ready..."
sleep 15

# Create Kafka topics (remove -it for non-interactive mode)
KAFKA_CONTAINER=$(docker ps -q -f name=kafka)
if [ -n "$KAFKA_CONTAINER" ]; then
    docker exec $KAFKA_CONTAINER kafka-topics --create --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || true
    docker exec $KAFKA_CONTAINER kafka-topics --create --topic content-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || true
fi

echo "✓ Infrastructure started"

# Activate virtual environment
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "Error: Virtual environment not found. Please run ./build.sh first."
    exit 1
fi

source "$SCRIPT_DIR/venv/bin/activate"

# Start services in background with full paths
cd "$SCRIPT_DIR/user_service"
python api.py > "$SCRIPT_DIR/logs/user_service.log" 2>&1 &
USER_PID=$!
cd "$SCRIPT_DIR"

sleep 3

cd "$SCRIPT_DIR/content_service"
python api.py > "$SCRIPT_DIR/logs/content_service.log" 2>&1 &
CONTENT_PID=$!
cd "$SCRIPT_DIR"

sleep 3

cd "$SCRIPT_DIR/notification_service"
python api.py > "$SCRIPT_DIR/logs/notification_service.log" 2>&1 &
NOTIFICATION_PID=$!
cd "$SCRIPT_DIR"

sleep 3

cd "$SCRIPT_DIR/monitoring"
python dashboard.py > "$SCRIPT_DIR/logs/dashboard.log" 2>&1 &
DASHBOARD_PID=$!
cd "$SCRIPT_DIR"

echo $USER_PID > "$SCRIPT_DIR/pids/user.pid"
echo $CONTENT_PID > "$SCRIPT_DIR/pids/content.pid"
echo $NOTIFICATION_PID > "$SCRIPT_DIR/pids/notification.pid"
echo $DASHBOARD_PID > "$SCRIPT_DIR/pids/dashboard.pid"

echo "✓ All services started"
echo "  User Service: http://localhost:8001"
echo "  Content Service: http://localhost:8002"
echo "  Notification Service: http://localhost:8003"
echo "  Dashboard: http://localhost:8000"
