#!/bin/bash

echo "=========================================="
echo "Interactive Queries Demo"
echo "=========================================="

source venv/bin/activate

echo ""
echo "Step 1: Verifying system health..."
curl -s http://localhost:8080/api/health | python -m json.tool

echo ""
echo "Step 2: Generating sample posts..."
python src/post_generator.py &
GENERATOR_PID=$!

echo "Generating posts for 30 seconds..."
sleep 30
kill $GENERATOR_PID

echo ""
echo "Step 3: Querying trending topics..."
curl -s http://localhost:8080/api/trending | python -m json.tool

echo ""
echo "Step 4: Query specific hashtag (AI)..."
curl -s http://localhost:8080/api/trending/ai | python -m json.tool

echo ""
echo "Step 5: Checking cluster metadata..."
curl -s http://localhost:8080/api/metadata | python -m json.tool

echo ""
echo "=========================================="
echo "Demo complete!"
echo "Dashboard still running at:"
echo "http://localhost:8080/web/dashboard.html"
echo "=========================================="
