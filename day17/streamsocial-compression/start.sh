#!/bin/bash

echo "🚀 Starting StreamSocial Compression Lab"
echo "======================================"

# Check if running with Docker
if [ "$1" = "--docker" ]; then
    echo "Starting with Docker..."
    cd docker
    docker-compose up -d
    cd ..
    echo "✅ Services started with Docker"
    echo "📊 Dashboard: http://localhost:8080"
    echo "🔧 Redis: localhost:6379"
else
    echo "Starting with virtual environment..."
    source venv/bin/activate
    
    # Start Redis in background (if available)
    if command -v redis-server &> /dev/null; then
        echo "Starting Redis..."
        redis-server --daemonize yes --port 6379
    fi
    
    # Start the application
    echo "Starting Flask application..."
    export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
    cd src && python app.py &
    APP_PID=$!
    cd ..
    
    echo "✅ Application started (PID: $APP_PID)"
    echo "📊 Dashboard: http://localhost:8080"
    
    # Save PID for stop script
    echo $APP_PID > .app_pid
fi

echo ""
echo "🎯 Ready for compression analysis!"
echo "Try different data types and algorithms to see performance differences."
