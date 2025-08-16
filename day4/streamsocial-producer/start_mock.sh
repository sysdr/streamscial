#!/bin/bash

echo "🚀 Starting StreamSocial Kafka Producer Demo (Mock Mode)"
echo "========================================================"

# Activate virtual environment
source venv/bin/activate

# Run tests first (excluding integration tests that need Docker)
echo "🧪 Running unit tests..."
python -m pytest tests/unit/ -v

# Start mock demo
echo "🎬 Starting producer demo (mock mode)..."
echo "📊 Dashboard will be available at: http://localhost:8050"
echo "📈 Metrics API will be available at: http://localhost:8080/metrics"
echo "⚠️  Running in mock mode - no actual Kafka connection"
echo ""
echo "Press Ctrl+C to stop the demo"

python src/demo_mock.py
