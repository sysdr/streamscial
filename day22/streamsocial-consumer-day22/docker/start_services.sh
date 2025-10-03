#!/bin/bash

# Start both consumer and dashboard services
echo "Starting StreamSocial services..."

# Add current directory to Python path
export PYTHONPATH=/app:$PYTHONPATH

# Start the dashboard in the background
python src/monitoring/dashboard.py &
DASHBOARD_PID=$!

# Start the consumer (this will run in foreground)
echo "Starting kafka-python consumer process..."
python src/consumer/kafka_python_consumer.py &
CONSUMER_PID=$!
echo "Consumer started with PID: $CONSUMER_PID"

# Function to handle shutdown
cleanup() {
    echo "Shutting down services..."
    kill $DASHBOARD_PID 2>/dev/null
    kill $CONSUMER_PID 2>/dev/null
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Wait for either process to exit
echo "Waiting for consumer process..."
wait $CONSUMER_PID
echo "Consumer process exited"
