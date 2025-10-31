#!/bin/bash

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🎯 StreamSocial Connect Architecture Demo"
echo "========================================"

# Function to check service health
check_service() {
    local name=$1
    local url=$2
    if curl -s "$url" > /dev/null; then
        echo "✅ $name is healthy"
        return 0
    else
        echo "❌ $name is not responding"
        return 1
    fi
}

# Check Connect cluster health
echo ""
echo "📊 Checking Connect Cluster Health:"
check_service "Connect Worker 1" "http://localhost:8083"
check_service "Connect Worker 2" "http://localhost:8084"  
check_service "Connect Worker 3" "http://localhost:8085"

# Deploy social signals connector
echo ""
echo "🔌 Deploying Social Signals Connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @"$SCRIPT_DIR/connectors/social-signals-connector.json"

sleep 5

# Check connector status
echo ""
echo "📈 Connector Status:"
curl -s http://localhost:8083/connectors/social-signals-source/status | python -m json.tool

# Show active connectors across workers
echo ""
echo "🔗 Active Connectors Across Workers:"
for port in 8083 8084 8085; do
    echo "Worker on port $port:"
    curl -s http://localhost:$port/connectors || echo "  Not responding"
done

# Display sample data flow
echo ""
echo "📊 Sample Data Flowing Through System:"
echo "(Check dashboard at http://localhost:5000 for real-time monitoring)"

# Performance metrics
echo ""
echo "⚡ Performance Metrics:"
echo "- 3 Connect workers for high availability"
echo "- Automatic task distribution and failover"
echo "- Real-time social signal processing"
echo "- Distributed offset management"

echo ""
echo "🎯 Demo completed! Visit http://localhost:5000 for live monitoring"
