#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

source venv/bin/activate

echo "========================================="
echo "Day 56: TLS Encryption Demo"
echo "========================================="

# Start monitoring dashboard in background
cd "$SCRIPT_DIR/monitoring"
python3 security_dashboard.py &
DASHBOARD_PID=$!
cd "$SCRIPT_DIR"

sleep 3
echo "Dashboard running at http://localhost:5000"

# Send encrypted messages
echo ""
echo "Sending encrypted messages..."
python3 "$SCRIPT_DIR/src/producer/secure_producer.py" localhost:9093 "$SCRIPT_DIR/certs" &
PRODUCER_PID=$!

sleep 5

# Consume messages
echo ""
echo "Consuming encrypted messages..."
timeout 10 python3 "$SCRIPT_DIR/src/consumer/secure_consumer.py" localhost:9093 "$SCRIPT_DIR/certs" || true

# Show certificate info
echo ""
echo "========================================="
echo "Certificate Information"
echo "========================================="
openssl x509 -in "$SCRIPT_DIR/certs/broker-cert.pem" -noout -subject -issuer -dates

echo ""
echo "Demo complete! Visit http://localhost:5000 for security dashboard"
echo "Press Ctrl+C to stop"

wait $DASHBOARD_PID
