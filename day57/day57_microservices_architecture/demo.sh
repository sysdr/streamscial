#!/bin/bash

echo "=========================================="
echo "Day 57: Microservices Event Architecture"
echo "=========================================="
echo ""

source venv/bin/activate

echo "1. Registering user..."
# Use timestamp to ensure unique email
TIMESTAMP=$(date +%s)
USER_RESPONSE=$(curl -s -X POST http://localhost:8001/users \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"demo${TIMESTAMP}@streamsocial.com\",\"username\":\"demouser${TIMESTAMP}\"}")
USER_ID=$(echo $USER_RESPONSE | python -c "import sys, json; data=json.load(sys.stdin); print(data.get('user', {}).get('user_id', ''))" 2>/dev/null || echo "")
if [ -z "$USER_ID" ]; then
    echo "⚠ User registration failed or user already exists"
    echo "Response: $USER_RESPONSE"
    # Try to get existing user
    USER_RESPONSE=$(curl -s "http://localhost:8001/users?email=demo${TIMESTAMP}@streamsocial.com" 2>/dev/null || curl -s "http://localhost:8001/users" | python -c "import sys, json; users=json.load(sys.stdin).get('users', []); print(json.dumps(users[0] if users else {}))" 2>/dev/null)
    USER_ID=$(echo $USER_RESPONSE | python -c "import sys, json; data=json.load(sys.stdin); print(data.get('user_id', ''))" 2>/dev/null || echo "")
fi
if [ -z "$USER_ID" ]; then
    echo "❌ Failed to get user ID. Cannot proceed with demo."
    exit 1
fi
echo "✓ User ID: $USER_ID"
echo ""

sleep 3

echo "2. Creating post..."
POST_RESPONSE=$(curl -s -X POST http://localhost:8002/posts \
  -H "Content-Type: application/json" \
  -d "{\"user_id\":\"$USER_ID\",\"content\":\"Hello from microservices!\"}")
POST_ID=$(echo $POST_RESPONSE | python -c "import sys, json; print(json.load(sys.stdin)['post']['post_id'])" 2>/dev/null || echo "")
echo "✓ Post created: $POST_ID"
echo ""

sleep 3

echo "3. Liking post..."
curl -s -X POST "http://localhost:8002/posts/$POST_ID/like" \
  -H "Content-Type: application/json" \
  -d "{\"user_id\":\"$USER_ID\"}" > /dev/null
echo "✓ Post liked"
echo ""

sleep 3

echo "4. Checking notifications..."
curl -s "http://localhost:8003/notifications/$USER_ID" | python -m json.tool
echo ""

echo "5. Service Metrics:"
echo ""
echo "User Service:"
curl -s http://localhost:8001/metrics | python -m json.tool
echo ""
echo "Content Service:"
curl -s http://localhost:8002/metrics | python -m json.tool
echo ""
echo "Notification Service:"
curl -s http://localhost:8003/metrics | python -m json.tool
echo ""

echo "=========================================="
echo "Demo completed!"
echo "Open http://localhost:8000 for live dashboard"
echo "=========================================="
