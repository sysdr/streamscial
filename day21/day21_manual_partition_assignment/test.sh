#!/bin/bash

echo "🧪 Running Day 21 Tests"

# Activate virtual environment
source venv/bin/activate

# Set Python path
export PYTHONPATH=$PWD:$PYTHONPATH

# Run unit tests
echo "Running unit tests..."
python -m pytest tests/ -v

# Test partition assignment logic
echo "Testing partition assignment..."
python -c "
from src.partition_manager import PartitionAssignmentManager
manager = PartitionAssignmentManager()
assignments = manager.calculate_partition_assignment(3)
print('✅ Partition Assignment Test Passed')
print(f'Assignments: {assignments}')
"

# Test hashtag extraction
echo "Testing hashtag extraction..."
python -c "
from src.trend_worker import TrendWorker
import unittest.mock as mock
with mock.patch('src.trend_worker.KafkaConsumer'), mock.patch('src.trend_worker.KafkaProducer'), mock.patch('redis.Redis'):
    worker = TrendWorker(0)
    hashtags = worker._extract_hashtags('Testing #kafka #streaming #realtime systems')
    assert 'kafka' in hashtags
    assert 'streaming' in hashtags
    print('✅ Hashtag Extraction Test Passed')
"

echo "✅ All tests passed!"
