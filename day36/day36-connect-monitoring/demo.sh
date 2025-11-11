#!/bin/bash

echo "🎬 Running Connect Monitoring Demo"

cd "$(dirname "$0")"
source venv/bin/activate

echo ""
echo "=== Demo Steps ==="
echo "1. Starting PostgreSQL..."
docker-compose up -d postgres
sleep 5

echo "2. Running build and tests..."
./build.sh

echo ""
echo "3. Starting monitoring system..."
./start.sh
sleep 5

echo ""
echo "4. Generating sample data..."
echo "   Monitoring service is collecting metrics every 15 seconds"
echo "   Dashboard updates every 5 seconds"

echo ""
echo "5. Access the dashboard:"
echo "   🌐 Dashboard: http://localhost:5000"
echo "   📈 Prometheus: http://localhost:8000/metrics"

echo ""
echo "6. Watch real-time updates..."
echo "   - Health scores for 3 connectors"
echo "   - Throughput graphs"
echo "   - Error rate monitoring"
echo "   - Lag metrics"
echo "   - Active alerts"

echo ""
echo "Press Ctrl+C to stop the demo"

# Keep demo running
wait
