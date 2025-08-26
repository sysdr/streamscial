#!/bin/bash

echo "🔨 Building and Testing StreamSocial Rebalancing System..."

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Run unit tests
echo "🧪 Running unit tests..."
python -m pytest tests/unit/ -v --tb=short

# Run integration tests  
echo "🔗 Running integration tests..."
python -m pytest tests/integration/ -v --tb=short

# Check code structure
echo "🏗️ Verifying project structure..."
python -c "
import os
import sys

required_files = [
    'src/main.py',
    'src/consumers/feed_consumer.py', 
    'src/producers/interaction_producer.py',
    'src/listeners/rebalance_listener.py',
    'src/monitoring/lag_monitor.py',
    'src/monitoring/dashboard.py',
    'config/kafka_config.py',
    'docker/Dockerfile',
    'docker/docker-compose.yml'
]

missing_files = []
for file in required_files:
    if not os.path.exists(file):
        missing_files.append(file)

if missing_files:
    print('❌ Missing required files:')
    for file in missing_files:
        print(f'  - {file}')
    sys.exit(1)
else:
    print('✅ All required files present')
"

# Test import structure
echo "🔍 Testing import structure..."
python -c "
try:
    from src.main import StreamSocialRebalancingDemo
    from src.consumers.feed_consumer import FeedGeneratorConsumer  
    from src.listeners.rebalance_listener import StreamSocialRebalanceListener
    print('✅ All imports successful')
except ImportError as e:
    print(f'❌ Import error: {e}')
    exit(1)
"

echo "✅ Build and test completed successfully!"
echo "🚀 Run './start.sh' to start the system"
