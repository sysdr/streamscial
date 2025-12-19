#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=================================================="
echo "Starting ELK Stack and Kafka Infrastructure"
echo "=================================================="

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "Error: docker-compose.yml not found in $SCRIPT_DIR"
    exit 1
fi

# Start Docker containers
echo "Starting Docker containers..."
docker-compose up -d

echo ""
echo "Waiting for services to be ready..."
sleep 30

echo ""
echo "Checking service health..."

# Check Elasticsearch
echo -n "Elasticsearch: "
if curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; then
    echo "✓ Ready"
else
    echo "✗ Not ready"
fi

# Check Kafka
echo -n "Kafka: "
if docker exec day53-kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "✓ Ready"
else
    echo "✗ Not ready"
fi

# Check Kibana
echo -n "Kibana: "
if curl -s http://localhost:5601/api/status > /dev/null 2>&1; then
    echo "✓ Ready"
else
    echo "✗ Not ready (may take additional time)"
fi

# Check Redis
echo -n "Redis: "
if docker exec day53-redis redis-cli ping > /dev/null 2>&1; then
    echo "✓ Ready"
else
    echo "✗ Not ready"
fi

echo ""
echo "Infrastructure started!"
echo ""
echo "Access points:"
echo "- Elasticsearch: http://localhost:9200"
echo "- Kibana: http://localhost:5601"
echo "- Kafka: localhost:9092"
echo "- Logstash: localhost:5044"
echo "- Redis: localhost:6379"
echo ""
echo "Run ./demo.sh to start the demonstration"
