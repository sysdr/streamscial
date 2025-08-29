#!/bin/bash

echo "🚀 Starting StreamSocial Payment Processing System"

# Create virtual environment
echo "📦 Setting up Python 3.12 virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Start infrastructure
echo "🐳 Starting Kafka and Redis infrastructure..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 15

# Setup Kafka topics
echo "📝 Creating Kafka topics..."
python scripts/setup_kafka.py

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Start the application
echo "🌟 Starting application..."
python main.py &
APP_PID=$!

echo "✅ StreamSocial Payment System is running!"
echo "🌐 Dashboard: http://localhost:8080"
echo "📊 API Stats: http://localhost:8080/api/stats"
echo ""
echo "🛑 To stop the system, run: ./stop.sh"

# Keep the script running
wait $APP_PID
