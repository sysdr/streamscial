#!/bin/bash

echo "🔨 Building StreamSocial Day 20: Kafka Replication & ISR Management"
echo "=================================================================="

# Activate virtual environment
source venv/bin/activate

# Check Python dependencies
echo "📋 Checking Python dependencies..."
pip check

# Start Docker services
echo "🐳 Starting Docker services..."
cd docker
docker-compose down --remove-orphans
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for Kafka cluster to be ready..."
sleep 30

# Verify Kafka brokers are up
echo "🔍 Verifying Kafka brokers..."
BROKERS="localhost:9091 localhost:9092 localhost:9093 localhost:9094 localhost:9095"
for broker in $BROKERS; do
    echo "Checking $broker..."
    timeout 10 bash -c "echo > /dev/tcp/${broker%:*}/${broker#*:}" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "✅ $broker is ready"
    else
        echo "⚠️ $broker is not ready"
    fi
done

cd ..

# Create topics and test basic functionality
echo "🏗️ Creating Kafka topics..."
python3 -c "
from src.producers.streamsocial_producer import StreamSocialProducer
producer = StreamSocialProducer('localhost:9091,localhost:9092,localhost:9093')
producer.create_topics()
print('✅ Topics created successfully')
"

echo "✅ Build completed successfully!"
echo "Next steps:"
echo "  1. Run tests: python -m pytest tests/ -v"
echo "  2. Start monitoring: python dashboard/app.py"
echo "  3. Run demo: python scripts/demo.py"
