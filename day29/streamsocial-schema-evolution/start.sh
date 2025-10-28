#!/bin/bash
set -e

echo "🚀 StreamSocial Schema Evolution - Start Script"
echo "=============================================="

# Start Docker infrastructure
echo "🐳 Starting Docker infrastructure..."
cd docker
docker compose up -d
echo "✅ Docker services starting..."

# Wait for services
echo "⏳ Waiting for services to be ready..."
echo "   - Zookeeper: localhost:2181"
echo "   - Kafka: localhost:9092" 
echo "   - Schema Registry: localhost:8081"
echo "   - Kafka UI: localhost:8080"

# Check services health
check_service() {
    local service=$1
    local port=$2
    local max_attempts=30
    
    for attempt in $(seq 1 $max_attempts); do
        if nc -z localhost $port 2>/dev/null; then
            echo "✅ $service is ready"
            return 0
        fi
        echo "   ⏳ Waiting for $service (attempt $attempt/$max_attempts)..."
        sleep 2
    done
    
    echo "❌ $service failed to start"
    return 1
}

# Wait for all services
check_service "Zookeeper" 2181
check_service "Kafka" 9092
check_service "Schema Registry" 8081

cd ..

# Activate virtual environment
echo "🌐 Activating virtual environment..."
source venv/bin/activate

# Create Kafka topic
echo "📋 Creating Kafka topic..."
timeout 30s bash -c 'until kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-connection < /dev/null 2>/dev/null; do sleep 2; done' || true

# Setup schemas
echo "📝 Setting up schemas..."
python -c "
from src.registry.schema_client import SchemaRegistryClient
from src.producer.profile_producer import StreamSocialProducer
import time

# Wait a bit more for schema registry
time.sleep(5)

try:
    producer = StreamSocialProducer()
    producer.setup_schemas()
    print('✅ Schemas registered successfully')
except Exception as e:
    print(f'⚠️  Schema setup warning: {e}')
"

echo ""
echo "🎉 All services are ready!"
echo ""
echo "🔗 Service URLs:"
echo "   - Kafka UI: http://localhost:8080"
echo "   - Schema Registry: http://localhost:8081"
echo "   - Dashboard: http://localhost:8050 (when started)"
echo ""
echo "🧪 Run tests:"
echo "   python -m pytest tests/ -v"
echo ""
echo "🎨 Start dashboard:"
echo "   python src/dashboard/app.py"
echo ""
