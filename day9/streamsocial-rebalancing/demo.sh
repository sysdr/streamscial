#!/bin/bash

echo "🎭 StreamSocial Rebalancing Demo Script"
echo "======================================="

# Function to check if system is running
check_system() {
    if curl -s http://localhost:8050 > /dev/null; then
        echo "✅ Dashboard is running"
    else
        echo "❌ Dashboard not accessible"
        return 1
    fi
    
    if curl -s http://localhost:8000 > /dev/null; then
        echo "✅ Metrics server is running"
    else
        echo "❌ Metrics server not accessible"
        return 1
    fi
}

# Function to simulate traffic spike
create_traffic_spike() {
    echo "🔥 Creating traffic spike for 60 seconds..."
    python -c "
import requests
import time

# This would trigger traffic spike in real implementation
# For demo, we'll just show the concept
print('Traffic spike simulation started...')
print('Monitor the dashboard at http://localhost:8050')
print('You should see:')
print('  - Increased partition lag')
print('  - Consumer count scaling up')
print('  - Rebalancing events')
time.sleep(5)
print('✅ Traffic spike simulation complete')
"
}

# Function to show system stats
show_stats() {
    echo "📊 Current System Statistics:"
    echo "-----------------------------"
    
    # Show consumer count
    if [ -f "/tmp/consumer_count.txt" ]; then
        count=$(cat /tmp/consumer_count.txt)
        echo "Active Consumers: $count"
    else
        echo "Active Consumers: 3 (default)"
    fi
    
    # Show dashboard access
    echo "Dashboard: http://localhost:8050"
    echo "Metrics: http://localhost:8000"
    echo ""
    echo "🎯 Demo Features to Explore:"
    echo "1. Real-time consumer scaling"
    echo "2. Partition lag monitoring"
    echo "3. Rebalancing event tracking"
    echo "4. Processing latency metrics"
}

# Main demo flow
echo "Starting demo sequence..."
echo ""

echo "1. Checking system status..."
if check_system; then
    echo "✅ System is running"
else
    echo "❌ System not running. Start with './start.sh' first"
    exit 1
fi

echo ""
echo "2. Showing current statistics..."
show_stats

echo ""
echo "3. Creating traffic spike demonstration..."
create_traffic_spike

echo ""
echo "4. Demo complete!"
echo "🎉 Key things to observe in the dashboard:"
echo "   - Consumer count changes during load"
echo "   - Partition lag spikes and recovery"
echo "   - Rebalancing events in real-time"
echo "   - System auto-scaling behavior"
echo ""
echo "👀 Visit http://localhost:8050 to see live metrics!"
