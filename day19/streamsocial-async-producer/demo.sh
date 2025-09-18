#!/bin/bash

echo "🎯 Running StreamSocial Async Producer Demo..."

# Ensure app is running
if ! curl -s http://localhost:5000/health > /dev/null; then
    echo "❌ Application not running. Please run './start.sh' first"
    exit 1
fi

echo ""
echo "1️⃣ Checking application health..."
curl -s http://localhost:5000/health | python3 -m json.tool

echo ""
echo "2️⃣ Initial metrics..."
curl -s http://localhost:5000/metrics | python3 -m json.tool

echo ""
echo "3️⃣ Submitting test posts..."

# Submit various test posts
test_posts=(
    '{"user_id": "user123", "content": "Beautiful sunset today!"}'
    '{"user_id": "vip_alice", "content": "VIP user posting important content"}'
    '{"user_id": "user456", "content": "This might be spam content"}'
    '{"user_id": "user789", "content": "Regular user sharing thoughts"}'
)

for post in "${test_posts[@]}"; do
    echo "📝 Posting: $post"
    curl -s -X POST http://localhost:5000/post \
        -H "Content-Type: application/json" \
        -d "$post" | python3 -m json.tool
    sleep 1
done

echo ""
echo "4️⃣ Updated metrics after submissions..."
sleep 2
curl -s http://localhost:5000/metrics | python3 -m json.tool

echo ""
echo "5️⃣ Running mini load test..."
python tests/load_test.py

echo ""
echo "✅ Demo complete!"
echo ""
echo "🌐 Open http://localhost:5000/dashboard to see the live dashboard"
echo "📊 Monitor metrics at http://localhost:5000/metrics"
