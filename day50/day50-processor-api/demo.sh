#!/bin/bash

echo "========================================="
echo "Day 50: Processor API Demo"
echo "========================================="

API_URL="http://localhost:8080"

echo -e "\n1. Checking API health..."
curl -s ${API_URL}/api/health | python3 -m json.tool

sleep 5

echo -e "\n\n2. Fetching processing metrics..."
curl -s ${API_URL}/api/metrics | python3 -m json.tool | head -30

sleep 3

echo -e "\n\n3. Querying recommendations for user_100..."
curl -s ${API_URL}/api/recommendations/user_100 | python3 -m json.tool

sleep 3

echo -e "\n\n4. Checking feature store status..."
curl -s ${API_URL}/api/state/feature-store | python3 -m json.tool

sleep 3

echo -e "\n\n5. Checking score store status..."
curl -s ${API_URL}/api/state/score-store | python3 -m json.tool

echo -e "\n\n✅ Demo complete!"
echo -e "\nVisit the dashboard at: http://localhost:8000"
echo -e "API Swagger docs at: http://localhost:8080/api/metrics"
