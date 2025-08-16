#!/bin/bash

echo "🚀 Starting StreamSocial Kafka Producer Demo"
echo "============================================"

# Activate virtual environment
source venv/bin/activate

# Start Kafka if not running
echo "🐳 Starting Kafka cluster..."
cd docker
if docker-compose up -d; then
    echo "✅ Kafka cluster started successfully"
    cd ..
    
    # Wait for Kafka to be ready
    echo "⏳ Waiting for Kafka to be ready..."
    sleep 30
    
    # Verify Kafka topics exist
    echo "📝 Verifying Kafka topics..."
    if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092; then
        echo "✅ Kafka topics verified"
    else
        echo "⚠️  Kafka topics verification failed"
        cd ..
        echo "🔄 Falling back to mock demo..."
        ./start_mock.sh
        exit 0
    fi
else
    echo "⚠️  Failed to start Kafka cluster"
    cd ..
    echo "🔄 Falling back to mock demo..."
    ./start_mock.sh
    exit 0
fi
cd ..

# Run tests first
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Start demo
echo "🎬 Starting producer demo..."
echo "📊 Dashboard will be available at: http://localhost:8050"
echo "📈 Metrics API will be available at: http://localhost:8080/metrics"
echo ""
echo "Press Ctrl+C to stop the demo"

# Try to start the real demo, fallback to mock if Kafka is not available
if python src/demo.py; then
    echo "✅ Real demo completed successfully"
else
    echo "⚠️  Real demo failed, starting mock demo..."
    ./start_mock.sh
fi
