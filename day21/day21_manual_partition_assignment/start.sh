#!/bin/bash

echo "🚀 Starting StreamSocial Trend Analysis System"

# Activate virtual environment
source venv/bin/activate

# Set Python path
export PYTHONPATH=$PWD:$PYTHONPATH

# Start with Docker (recommended)
if command -v docker-compose &> /dev/null; then
    echo "Starting with Docker Compose..."
    cd docker
    docker-compose up -d zookeeper kafka redis
    echo "Waiting for services to start..."
    sleep 10
    cd ..
    
    echo "Starting trend analysis system..."
    python -m src.main &
    MAIN_PID=$!
    
    echo "Starting web dashboard..."
    cd web
    python dashboard.py &
    WEB_PID=$!
    cd ..
    
    echo "✅ System started!"
    echo "🌐 Dashboard: http://localhost:5001"
    echo "📊 Kafka UI: http://localhost:9092"
    echo ""
    echo "Press Ctrl+C to stop..."
    
    # Wait for user interrupt
    trap "echo 'Stopping system...'; kill $MAIN_PID $WEB_PID; cd docker; docker-compose down; exit" INT
    wait
else
    echo "❌ Docker not found. Please install Docker and Docker Compose."
    echo "Alternatively, start Kafka and Redis manually and run:"
    echo "  python -m src.main &"
    echo "  cd web && python dashboard.py"
fi
