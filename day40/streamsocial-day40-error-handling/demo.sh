#!/bin/bash

echo "=========================================="
echo "StreamSocial Day 40: Error Handling Demo"
echo "=========================================="

source venv/bin/activate

echo ""
echo "Starting demo scenario..."
echo "1. Producing 100 test posts (some will fail)"
echo "2. Watch moderation connector handle errors"
echo "3. Observe DLQ retry service processing"
echo "4. View metrics on dashboard"
echo ""

python src/test_data_producer.py

echo ""
echo "=========================================="
echo "Demo Complete"
echo "=========================================="
echo ""
echo "View dashboard at: http://localhost:5000"
echo ""
echo "Key observations:"
echo "- ~15% of posts will encounter errors"
echo "- Retryable errors (timeout, service down) are retried"
echo "- Non-retryable errors (Unicode) go to DLQ immediately"
echo "- Circuit breaker may trip if errors spike"
echo "- DLQ retry service processes failed records with backoff"
echo ""
