#!/bin/bash

set -e

echo "========================================="
echo "Building Day 55: Kafka ACLs"
echo "========================================="

# Create virtual environment
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

echo "✓ Build completed successfully"
echo ""
echo "Next steps:"
echo "1. Start Kafka with ACLs: docker-compose up -d"
echo "2. Wait 30 seconds for Kafka to be ready"
echo "3. Provision ACLs: python src/acl_manager/provision_acls.py"
echo "4. Run tests: pytest tests/ -v"
echo "5. Start dashboard: python src/monitoring/acl_dashboard.py"
echo "6. Run demo: python src/demo.py"
