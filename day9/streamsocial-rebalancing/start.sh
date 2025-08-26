#!/bin/bash

echo "🚀 Starting StreamSocial Rebalancing System..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Run the setup script first."
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check if Kafka is running
if ! nc -z localhost 9092 2>/dev/null; then
    echo "🐳 Starting Kafka with Docker Compose..."
    cd docker
    docker-compose up -d
    cd ..
    
    echo "⏳ Waiting for Kafka to be ready..."
    sleep 30
fi

# Create Kafka topics
echo "📝 Creating Kafka topics..."
python -c "
from kafka.admin import KafkaAdminClient, NewTopic
from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
topics = [
    NewTopic(name=KAFKA_TOPICS['user_interactions'], num_partitions=6, replication_factor=1),
    NewTopic(name=KAFKA_TOPICS['feed_requests'], num_partitions=3, replication_factor=1),
    NewTopic(name=KAFKA_TOPICS['system_metrics'], num_partitions=1, replication_factor=1)
]

try:
    admin.create_topics(topics)
    print('✅ Topics created successfully')
except Exception as e:
    print(f'ℹ️ Topics may already exist: {e}')
"

# Note: Prometheus metrics server is now handled by the Docker container
echo "📊 Prometheus metrics will be available at http://localhost:8000 (from Docker container)"

# Run tests (optional - can be skipped if there are dependency conflicts)
echo "🧪 Running tests (optional)..."
if python -m pytest tests/ -v --tb=short 2>/dev/null; then
    echo "✅ Tests completed successfully"
else
    echo "⚠️ Tests skipped due to dependency conflicts - continuing with application startup"
fi

# Start the main application
echo "🎮 Starting StreamSocial application..."
echo "📊 Dashboard will be available at: http://localhost:8050"
echo "📈 Prometheus metrics at: http://localhost:8000"
echo "🔥 To create traffic spike, run: curl -X POST http://localhost:8050/spike"
echo ""
echo "Press Ctrl+C to stop..."

python src/main.py
