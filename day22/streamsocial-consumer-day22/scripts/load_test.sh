#!/bin/bash

echo "🚀 Load Testing StreamSocial Consumer"

# Activate virtual environment
source venv/bin/activate

# Set Python path to include project root
export PYTHONPATH=$(pwd):$PYTHONPATH

RATE=${1:-1000}
DURATION=${2:-60}

echo "📈 Running load test: $RATE messages/second for $DURATION seconds"
echo "🎯 Target: Maintain <50ms latency"

python src/utils/kafka_python_producer.py load $RATE $DURATION
