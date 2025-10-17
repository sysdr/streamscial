#!/bin/bash

echo "🚀 Starting StreamSocial Day 27 Infrastructure"

# Start Docker services
echo "🐳 Starting Kafka infrastructure..."
cd docker && docker-compose up -d

# Wait for services
echo "⏳ Waiting for services to be ready..."
sleep 30

# Verify services
echo "🔍 Verifying services..."
curl -s http://localhost:8081/subjects || echo "Schema Registry not ready"

# Build project
cd .. && ./build.sh

echo "✅ Infrastructure started!"
echo "🌐 Schema Registry: http://localhost:8081"
echo "🎯 Ready to run: ./demo.sh"
