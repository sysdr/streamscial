#!/bin/bash

# StreamSocial Timeline Ordering Test Script
set -e

echo "🧪 Testing StreamSocial Timeline Ordering System"
echo "=============================================="

# Activate virtual environment
source venv/bin/activate

# Set Python path
export PYTHONPATH="${PWD}:${PYTHONPATH}"

# Run unit tests
echo "🔬 Running unit tests..."
python -m pytest tests/unit/ -v --tb=short

echo ""
echo "🔗 Running integration tests..."
echo "⚠️  Note: Integration tests require running Kafka. Start Kafka first with './start.sh kafka' if needed."

# Check if Kafka is available for integration tests
python -c "
import socket
import sys

def check_kafka():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        return result == 0
    except:
        return False

if check_kafka():
    print('✅ Kafka is available - running integration tests')
else:
    print('⚠️  Kafka not available - skipping integration tests')
    print('   Start Kafka with: docker-compose -f docker/docker-compose.yml up -d kafka')
    sys.exit(0)
"

# Run integration tests only if Kafka is available
if [ $? -eq 0 ]; then
    python -m pytest tests/integration/ -v --tb=short
fi

echo ""
echo "🎯 Running basic functionality tests..."

# Test partition analyzer
python -c "
from src.utils.partition_analyzer import PartitionAnalyzer

analyzer = PartitionAnalyzer()
keys = ['user:alice', 'user:bob', 'user:charlie']
analysis = analyzer.analyze_key_distribution(keys)

print('✅ Partition Analyzer working correctly')
print(f'   Keys: {len(keys)}, Balance Score: {analysis[\"load_balance_score\"]:.2f}')
"

# Test message creation
python -c "
from src.producers.timeline_producer import TimelineMessage
import time

message = TimelineMessage('test_user', 'test_post', 'Test content')
print('✅ Timeline Message creation working correctly')
print(f'   User: {message.user_id}, Content: {message.content}')
print(f'   Timestamp: {message.timestamp}')
"

echo ""
echo "✅ All tests completed successfully!"
echo ""
echo "🚀 Ready for demo! Run './start.sh' to start the full system."
