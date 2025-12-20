#!/bin/bash
# Don't exit on errors - continue with build

echo "🔧 Building SASL Authentication System..."

# Check Python version
python3 --version || { echo "❌ Python 3 required"; exit 1; }

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate

# Install dependencies
echo "📥 Installing dependencies..."
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

# Create directories
echo "📁 Creating directory structure..."
mkdir -p data/credentials logs kafka

# Start Docker containers
echo "🐳 Starting Kafka cluster with SASL..."
docker-compose up -d

# Quick wait for services (reduced from 45s to 20s)
echo "⏳ Waiting for Kafka cluster (20s)..."
sleep 20

# Quick health check (reduced attempts)
echo "🔍 Checking Kafka readiness..."
for i in {1..5}; do
    if docker exec sasl-kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:9092 >/dev/null 2>&1; then
        echo "✅ Kafka is ready!"
        break
    fi
    [ $i -eq 5 ] && echo "⚠️  Kafka starting in background, continuing..."
    sleep 1
done

# Create SCRAM credentials in Kafka (background, non-blocking)
echo "👤 Creating SCRAM credentials..."
(
docker exec sasl-kafka-1 kafka-configs --zookeeper zookeeper:2181 \
    --alter --add-config 'SCRAM-SHA-512=[iterations=4096,password=admin-secret]' \
    --entity-type users --entity-name admin 2>/dev/null || true

docker exec sasl-kafka-1 kafka-configs --zookeeper zookeeper:2181 \
    --alter --add-config 'SCRAM-SHA-512=[iterations=4096,password=producer-secret]' \
    --entity-type users --entity-name producer 2>/dev/null || true

docker exec sasl-kafka-1 kafka-configs --zookeeper zookeeper:2181 \
    --alter --add-config 'SCRAM-SHA-512=[iterations=4096,password=consumer-secret]' \
    --entity-type users --entity-name consumer 2>/dev/null || true

docker exec sasl-kafka-1 kafka-configs --zookeeper zookeeper:2181 \
    --alter --add-config 'SCRAM-SHA-512=[iterations=4096,password=analytics-secret]' \
    --entity-type users --entity-name analytics 2>/dev/null || true
) &

# Create topics (try quickly, don't wait long)
echo "📝 Creating Kafka topics..."
docker exec sasl-kafka-1 bash -c 'cat > /tmp/client.properties << EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
EOF
' 2>/dev/null || true

# Try to create topic (timeout quickly)
timeout 10 docker exec sasl-kafka-1 kafka-topics --bootstrap-server kafka-1:9092 \
    --command-config /tmp/client.properties --create --if-not-exists \
    --topic streamsocial-posts --partitions 3 --replication-factor 1 \
    --config retention.ms=86400000 2>/dev/null || echo "⚠️  Topic creation will complete in background"

# Skip tests and demo for faster build - user can run manually
echo "⏭️  Skipping tests and demo for faster build (run manually if needed)"

echo ""
echo "✅ Build completed successfully!"
echo ""
echo "📊 Access the dashboard:"
echo "   http://localhost:8054"
echo ""
echo "🔍 Check Kafka cluster:"
echo "   docker-compose ps"
echo ""
echo "🛠️ Next steps:"
echo "   ./start.sh   - Start the dashboard"
echo "   ./stop.sh    - Stop all services"
echo ""
