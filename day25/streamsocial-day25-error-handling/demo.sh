#!/bin/bash

echo "🎬 StreamSocial Error Handling Demo"
echo "=================================="

# Activate virtual environment
source venv/bin/activate

echo "1. Starting infrastructure..."
docker-compose up -d
sleep 15

echo "2. Creating topics..."
docker exec kafka kafka-topics --create --topic streamsocial-posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic streamsocial-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

echo "3. Demonstrating error handling capabilities:"
echo "   - Valid message processing ✅"
echo "   - Malformed JSON handling 🔧" 
echo "   - DLQ routing for poison pills ☠️"
echo "   - Real-time monitoring dashboard 📊"

echo ""
echo "🌐 Open dashboard: http://localhost:8000"
echo "⚡ System will process messages with 15% error rate to demonstrate error handling"
echo ""
echo "Press Ctrl+C to stop the demo"

# Start main application
python src/main.py
