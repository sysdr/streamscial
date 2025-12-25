#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting TLS-enabled Kafka infrastructure..."

# Copy certificates to /tmp for Docker access
sudo mkdir -p /tmp/day56/certs
sudo cp -r certs/* /tmp/day56/certs/
sudo chmod -R 755 /tmp/day56

# Start with Docker
docker-compose up -d

echo "Waiting for Kafka to be ready..."
sleep 15

# Create topics
source venv/bin/activate
python3 << PYTHON
import os
from kafka.admin import KafkaAdminClient, NewTopic

script_dir = "$SCRIPT_DIR"
certs_dir = os.path.join(script_dir, 'certs')

admin = KafkaAdminClient(
    bootstrap_servers='localhost:9093',
    security_protocol='SSL',
    ssl_cafile=os.path.join(certs_dir, 'ca-cert.pem'),
    ssl_certfile=os.path.join(certs_dir, 'client-cert.pem'),
    ssl_keyfile=os.path.join(certs_dir, 'client-key.pem')
)

topics = [
    NewTopic('secure-messages', num_partitions=3, replication_factor=1),
    NewTopic('performance-test', num_partitions=3, replication_factor=1)
]

try:
    admin.create_topics(topics, timeout_ms=10000)
    print("Topics created successfully")
except Exception as e:
    print(f"Topics may already exist: {e}")

admin.close()
PYTHON

echo "Infrastructure ready!"
echo "Kafka SSL: localhost:9093"
echo "Kafka Plaintext: localhost:9092"
