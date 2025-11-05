#!/bin/bash
set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🚀 Starting StreamSocial Connect Cluster..."

# Check if services are already running
if docker-compose ps | grep -q "Up"; then
    echo "⚠️  Some services are already running. Checking for duplicates..."
    docker-compose ps
fi

# Start infrastructure
echo "📊 Starting Kafka infrastructure..."
docker-compose up -d zookeeper kafka schema-registry

echo "⏳ Waiting for Kafka to be ready..."
sleep 30

# Start Connect workers
echo "🔗 Starting Connect workers..."
docker-compose up -d connect-worker-1 connect-worker-2 connect-worker-3

echo "⏳ Waiting for Connect cluster to be ready..."
sleep 45

# Start dashboard
echo "📈 Starting monitoring dashboard..."
docker-compose up -d ui-dashboard

# Start data generator (if not already running)
echo "📊 Starting data generator..."
if [ -f "$SCRIPT_DIR/data_generator.pid" ]; then
    PID=$(cat "$SCRIPT_DIR/data_generator.pid")
    if ps -p $PID > /dev/null 2>&1; then
        echo "⚠️  Data generator is already running (PID: $PID)"
    else
        echo "📊 Starting new data generator..."
        source "$SCRIPT_DIR/venv/bin/activate"
        cd "$SCRIPT_DIR"
        nohup python "$SCRIPT_DIR/src/data_generator.py" > "$SCRIPT_DIR/data_generator.log" 2>&1 &
        echo $! > "$SCRIPT_DIR/data_generator.pid"
    fi
else
    source "$SCRIPT_DIR/venv/bin/activate"
    cd "$SCRIPT_DIR"
    nohup python "$SCRIPT_DIR/src/data_generator.py" > "$SCRIPT_DIR/data_generator.log" 2>&1 &
    echo $! > "$SCRIPT_DIR/data_generator.pid"
fi

echo ""
echo "✅ StreamSocial Connect Cluster is running!"
echo ""
echo "🌐 Access points:"
echo "  Dashboard:     http://localhost:5000"
echo "  Connect API:   http://localhost:8083, 8084, 8085"
echo "  Kafka:         localhost:9092"
echo "  Schema Registry: http://localhost:8081"
echo ""
echo "📋 Useful commands:"
echo "  docker-compose logs -f    - View logs"
echo "  $SCRIPT_DIR/test.sh                - Run tests"
echo "  $SCRIPT_DIR/stop.sh               - Stop everything"
