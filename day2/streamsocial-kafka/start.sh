#!/bin/bash
set -e

echo "🔧 Setting up StreamSocial Kafka Environment..."

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    python3.11 -m venv venv
fi
source venv/bin/activate

# Install dependencies (skip problematic docker-compose)
echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install kafka-python flask flask-cors requests psutil

echo "🐳 Attempting to start Docker Compose services..."
if docker compose -f docker/docker-compose.yml up -d; then
    echo "✅ Docker services started successfully"
    KAFKA_AVAILABLE=true
else
    echo "⚠️ Docker services failed to start - running in demo mode"
    echo "📝 This is normal if Docker is not available or has resource constraints"
    KAFKA_AVAILABLE=false
fi

# Wait for services to be ready (only if Kafka is available)
if [ "$KAFKA_AVAILABLE" = true ]; then
    echo "⏳ Waiting for Kafka cluster to be ready..."
    sleep 30
    
    # Run tests only if Kafka is available
    echo "🧪 Running cluster tests..."
    if python tests/test_cluster.py; then
        echo "✅ Cluster tests passed"
    else
        echo "⚠️ Cluster tests failed - continuing in demo mode"
        KAFKA_AVAILABLE=false
    fi
else
    echo "🎭 Running in demo mode - Kafka cluster not available"
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Kill any existing monitoring processes
echo "🔄 Stopping any existing monitoring processes..."
pkill -f "monitoring_dashboard.py" 2>/dev/null || true
pkill -f "http.server" 2>/dev/null || true

# Start monitoring dashboard with system monitoring
echo "📊 Starting enhanced monitoring dashboard with system monitoring..."
cd src
python monitoring_dashboard.py > ../logs/monitoring.log 2>&1 &
MONITOR_PID=$!
cd ..

# Wait a moment for the dashboard to start
sleep 3

# Verify monitoring dashboard is running
if ps -p $MONITOR_PID > /dev/null; then
    echo "✅ Monitoring dashboard started (PID: $MONITOR_PID)"
else
    echo "❌ Failed to start monitoring dashboard"
    echo "📝 Check logs/monitoring.log for details"
    exit 1
fi

# Start frontend on a different port
echo "🌐 Starting frontend..."
cd frontend
python -m http.server 3001 > ../logs/frontend.log 2>&1 &
FRONTEND_PID=$!
cd ..

# Wait a moment for the frontend to start
sleep 2

# Verify frontend is running
if ps -p $FRONTEND_PID > /dev/null; then
    echo "✅ Frontend started (PID: $FRONTEND_PID)"
else
    echo "❌ Failed to start frontend"
    echo "📝 Check logs/frontend.log for details"
    exit 1
fi

echo ""
echo "🎉 StreamSocial Kafka Environment Successfully Started!"
echo "=================================================="
echo "📊 Enhanced Monitoring Dashboard: http://localhost:5000"
echo "   - Real-time Kafka cluster monitoring"
echo "   - Live system metrics (CPU, Memory, Disk, Network)"
echo "   - Process monitoring and system events"
echo "   - Tabbed interface for different event types"
echo ""
echo "🌐 Frontend Dashboard: http://localhost:3001"
echo "   - Beautiful web interface"
echo "   - Real-time status updates"
echo "   - Interactive features"

if [ "$KAFKA_AVAILABLE" = true ]; then
    echo ""
    echo "🔗 Kafka Brokers: localhost:9092, localhost:9093, localhost:9094"
    echo "🎯 Full functionality available"
else
    echo ""
    echo "🎭 Demo mode active - UI components available, Kafka features simulated"
    echo "💡 To enable full functionality, ensure Docker is running and has sufficient resources"
fi

echo ""
echo "📈 System Monitoring Features:"
echo "   - CPU usage and frequency monitoring"
echo "   - Memory and swap usage tracking"
echo "   - Disk usage and I/O statistics"
echo "   - Network activity monitoring"
echo "   - Process count and top processes"
echo "   - System load averages"
echo "   - Battery status (if available)"
echo "   - Temperature sensors (if available)"
echo ""
echo "📝 Logs available in:"
echo "   - Monitoring: logs/monitoring.log"
echo "   - Frontend: logs/frontend.log"
echo ""
echo "Press Ctrl+C to stop all services"

# Store PIDs for cleanup
echo $MONITOR_PID > .monitor.pid
echo $FRONTEND_PID > .frontend.pid

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping StreamSocial services..."
    
    # Stop monitoring dashboard
    if [ -f .monitor.pid ]; then
        MONITOR_PID=$(cat .monitor.pid)
        if ps -p $MONITOR_PID > /dev/null; then
            echo "🛑 Stopping monitoring dashboard (PID: $MONITOR_PID)"
            kill $MONITOR_PID 2>/dev/null || true
        fi
        rm -f .monitor.pid
    fi
    
    # Stop frontend
    if [ -f .frontend.pid ]; then
        FRONTEND_PID=$(cat .frontend.pid)
        if ps -p $FRONTEND_PID > /dev/null; then
            echo "🛑 Stopping frontend (PID: $FRONTEND_PID)"
            kill $FRONTEND_PID 2>/dev/null || true
        fi
        rm -f .frontend.pid
    fi
    
    # Stop Docker services if they were started
    if [ "$KAFKA_AVAILABLE" = true ]; then
        echo "🛑 Stopping Docker services..."
        docker compose -f docker/docker-compose.yml down 2>/dev/null || true
    fi
    
    echo "✅ All services stopped"
    exit 0
}

# Set up signal handlers
trap cleanup INT TERM

# Wait for user interrupt
wait
