#!/bin/bash
set -e

echo "🔨 Building StreamSocial Headers System..."

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

# Create Kafka topics
echo "📝 Creating Kafka topics..."
docker-compose -f docker/docker-compose.yml exec kafka kafka-topics --create --topic posts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists || true

echo "✅ Build completed successfully!"
echo "💡 Next steps:"
echo "   1. Start services: ./start.sh"
echo "   2. Run demo: python src/demo.py"
echo "   3. Open dashboard: http://localhost:8000"
