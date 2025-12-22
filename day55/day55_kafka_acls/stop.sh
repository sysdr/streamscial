#!/bin/bash

echo "========================================="
echo "Stopping Day 55: Kafka ACLs"
echo "========================================="

# Stop Docker services
docker-compose down -v

echo "✓ All services stopped"
