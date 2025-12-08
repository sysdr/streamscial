#!/bin/bash

echo "Starting Table-Table Joins System..."

# Start Docker services
docker-compose up -d

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
sleep 15

# Create topics
docker exec kafka-day47 kafka-topics --create --topic user-preferences --bootstrap-server localhost:9093 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka-day47 kafka-topics --create --topic content-metadata --bootstrap-server localhost:9093 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka-day47 kafka-topics --create --topic content-recommendations --bootstrap-server localhost:9093 --partitions 3 --replication-factor 1 --if-not-exists

echo "Kafka topics created!"

# Activate virtual environment
source venv/bin/activate

# Start dashboard
cd dashboard
python app.py &
DASHBOARD_PID=$!
cd ..

sleep 3

# Start data producer
python src/data_producer.py &
PRODUCER_PID=$!

sleep 5

# Start join processor
python src/table_join_processor.py &
PROCESSOR_PID=$!

echo "======================================"
echo "System Started!"
echo "Dashboard: http://localhost:5000"
echo "======================================"
echo "PIDs: Dashboard=$DASHBOARD_PID Producer=$PRODUCER_PID Processor=$PROCESSOR_PID"
echo "Press Ctrl+C to stop"

# Save PIDs
echo $DASHBOARD_PID > /tmp/dashboard.pid
echo $PRODUCER_PID > /tmp/producer.pid
echo $PROCESSOR_PID > /tmp/processor.pid

wait
