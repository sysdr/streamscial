#!/bin/bash
# Demo script for StreamSocial Schema System

set -e

echo "🎭 Running StreamSocial Schema Demo..."

# Activate virtual environment
source venv/bin/activate

# Wait for services
echo "⏳ Waiting for services to be ready..."
sleep 5

# Test schema validation
echo "🧪 Testing schema validation..."

# Valid event test
echo "Testing valid user profile event..."
curl -X POST http://localhost:8001/validate \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "user.profile.v1",
    "event": {
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "timestamp": "2025-01-15T10:30:00Z",
      "event_type": "profile_created",
      "user_id": "demo_user",
      "profile_data": {
        "username": "demouser",
        "email": "demo@streamsocial.com",
        "privacy_settings": {
          "profile_visibility": "public",
          "allow_messages": true
        }
      },
      "metadata": {
        "source": "demo",
        "version": "1.0.0"
      }
    }
  }'

echo -e "\n"

# Invalid event test
echo "Testing invalid event (missing required field)..."
curl -X POST http://localhost:8001/validate \
  -H "Content-Type: application/json" \
  -d '{
    "schema_name": "user.profile.v1",
    "event": {
      "event_id": "550e8400-e29b-41d4-a716-446655440001",
      "timestamp": "2025-01-15T10:30:00Z",
      "event_type": "profile_created",
      "profile_data": {
        "username": "incomplete"
      }
    }
  }'

echo -e "\n"

# Get metrics
echo "Fetching validation metrics..."
curl http://localhost:8001/metrics

echo -e "\n"

# List schemas
echo "Listing registered schemas..."
curl http://localhost:8001/schemas

echo -e "\n✅ Demo completed! Check the dashboard at http://localhost:8501"
