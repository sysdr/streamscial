#!/bin/bash

echo "🚀 Starting StreamSocial Retry Logic System..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run the main script first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check if Kafka is running (with Docker)
echo "📋 Checking Kafka availability..."
docker ps | grep kafka > /dev/null
if [ $? -ne 0 ]; then
    echo "🐳 Starting Kafka with Docker Compose..."
    cd docker
    docker-compose up -d
    cd ..
    
    echo "⏳ Waiting for Kafka to be ready..."
    sleep 30
fi

# Create Kafka topics
echo "📝 Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic streamsocial-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic streamsocial-stories --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic streamsocial-reactions --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic streamsocial-comments --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1 --if-not-exists

# Start the application
echo "🎯 Starting StreamSocial Retry System..."
export PYTHONPATH=$PWD
python src/main.py

echo "✅ StreamSocial Retry System started successfully!"
echo "📊 Dashboard available at: http://localhost:5000"
