#!/bin/bash

echo "=========================================="
echo "Connect Cluster Failure Simulation"
echo "=========================================="

# Function to check cluster health
check_health() {
    echo -e "\nChecking cluster health..."
    curl -s http://localhost:8083/connectors | jq '.'
}

# Function to get task distribution
show_distribution() {
    echo -e "\nCurrent task distribution:"
    for connector in $(curl -s http://localhost:8083/connectors | jq -r '.[]'); do
        echo -e "\nConnector: $connector"
        curl -s "http://localhost:8083/connectors/$connector/status" | jq '.tasks[] | {id, state, worker_id}'
    done
}

echo -e "\n1. Initial cluster state:"
check_health
show_distribution

echo -e "\n\n2. Simulating Worker-2 graceful shutdown..."
docker stop connect-worker-2
echo "Waiting for rebalance (15 seconds)..."
sleep 15

echo -e "\n3. Cluster state after Worker-2 shutdown:"
show_distribution

echo -e "\n\n4. Restarting Worker-2..."
docker start connect-worker-2
echo "Waiting for rejoin and rebalance (15 seconds)..."
sleep 15

echo -e "\n5. Cluster state after Worker-2 restart:"
show_distribution

echo -e "\n\n6. Simulating Worker-3 crash (immediate kill)..."
docker kill connect-worker-3
echo "Waiting for failure detection and rebalance (15 seconds)..."
sleep 15

echo -e "\n7. Cluster state after Worker-3 crash:"
show_distribution

echo -e "\n\n8. Recovering Worker-3..."
docker start connect-worker-3
echo "Waiting for recovery (15 seconds)..."
sleep 15

echo -e "\n9. Final cluster state:"
check_health
show_distribution

echo -e "\n=========================================="
echo "Failure simulation complete!"
echo "=========================================="
