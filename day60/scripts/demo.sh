#!/bin/bash
set -e

echo "=========================================="
echo "Day 60: Production Readiness Demonstration"
echo "=========================================="
echo ""

# Activate virtual environment
source venv/bin/activate

# Demo 1: Capacity Planning
echo "[Demo 1/5] Capacity Planning for 100M DAU"
echo "==========================================="
python src/monitoring/capacity_planner.py
echo ""
read -p "Press Enter to continue..."

# Demo 2: Blue-Green Deployment
echo ""
echo "[Demo 2/5] Blue-Green Deployment Simulation"
echo "============================================"
python src/deployment/blue_green_deployer.py
echo ""
read -p "Press Enter to continue..."

# Demo 3: Disaster Recovery Drill
echo ""
echo "[Demo 3/5] Disaster Recovery Drill"
echo "==================================="
python src/disaster_recovery/failover_manager.py
echo ""
read -p "Press Enter to continue..."

# Demo 4: Load Testing
echo ""
echo "[Demo 4/5] Load Testing Scenarios"
echo "=================================="
python src/monitoring/load_tester.py
echo ""
read -p "Press Enter to continue..."

# Demo 5: Production Monitoring
echo ""
echo "[Demo 5/5] Production Monitoring Dashboard"
echo "==========================================="
echo "Starting real-time monitoring dashboard..."
echo "Open http://localhost:8000 in your browser"
echo ""
echo "Press Ctrl+C to stop the dashboard"
echo ""

python src/monitoring/production_dashboard.py
