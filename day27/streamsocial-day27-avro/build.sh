#!/bin/bash

echo "🏗️ Building StreamSocial Day 27 - Avro Serialization Project"

# Activate virtual environment
source venv_day27/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Create Kafka topics (requires running Kafka)
echo "📋 Creating Kafka topics..."
python3 -c "
from confluent_kafka.admin import AdminClient, NewTopic
import time

try:
    admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
    topics = [
        NewTopic('user-profiles', num_partitions=3, replication_factor=1),
        NewTopic('user-interactions', num_partitions=3, replication_factor=1),
        NewTopic('post-events', num_partitions=3, replication_factor=1)
    ]
    
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f'✅ Topic {topic} created')
        except Exception as e:
            print(f'⚠️ Topic {topic} already exists or error: {e}')
except Exception as e:
    print(f'⚠️ Kafka not available: {e}')
"

echo "✅ Build completed!"
