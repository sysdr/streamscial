#!/bin/bash

set -e

echo "========================================="
echo "Starting Day 55: Kafka ACLs"
echo "========================================="

# Start Docker services
echo "Starting Kafka with ACL support..."
docker-compose up -d

echo "Waiting for Kafka to be ready..."
# Wait for Kafka container to be running
max_attempts=90
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if docker-compose ps kafka 2>/dev/null | grep -q "Up"; then
        echo "Kafka container is running..."
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done

# Wait for Kafka broker to be ready (check if we can connect)
max_attempts=90
attempt=0
echo "Waiting for Kafka broker to accept connections..."
while [ $attempt -lt $max_attempts ]; do
    # Try to use kafka-broker-api-versions if available, otherwise check port
    if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>/dev/null | grep -q "9092"; then
        echo "Kafka broker is ready!"
        sleep 3  # Give it a bit more time to fully initialize
        break
    elif nc -z localhost 9092 2>/dev/null; then
        echo "Kafka port 9092 is open, waiting a bit more..."
        sleep 5
        break
    fi
    attempt=$((attempt + 1))
    if [ $((attempt % 10)) -eq 0 ]; then
        echo "Still waiting for Kafka... (${attempt}/${max_attempts})"
    fi
    sleep 1
done

if [ $attempt -eq $max_attempts ]; then
    echo "Warning: Kafka may not be fully ready, but continuing..."
    echo "Recent Kafka logs:"
    docker-compose logs kafka 2>&1 | tail -30
fi

# Activate virtual environment
source venv/bin/activate

# Provision ACLs
echo "Provisioning ACLs for all services..."
python src/acl_manager/provision_acls.py

echo ""
echo "✓ System started successfully"
echo ""
echo "Services:"
echo "- Kafka (SASL): localhost:9093"
echo "- Dashboard: http://localhost:5055"
echo ""
echo "To run demo: python src/demo.py"
echo "To run tests: pytest tests/ -v"
echo "To view dashboard: python src/monitoring/acl_dashboard.py &"
