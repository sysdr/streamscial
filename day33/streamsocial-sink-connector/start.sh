#!/bin/bash

set -e

echo "======================================"
echo "  Starting StreamSocial Infrastructure"
echo "======================================"

# Start Docker services
echo "Starting Docker services..."
cd docker
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 30

# Check Kafka
echo "Checking Kafka..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check PostgreSQL
echo "Checking PostgreSQL..."
docker-compose exec postgres psql -U postgres -d streamsocial -c "SELECT COUNT(*) FROM trending_hashtags;"

cd ..

echo "Infrastructure ready!"
echo ""
echo "Services running:"
echo "- Kafka: localhost:9092"
echo "- PostgreSQL: localhost:5432"
echo "- Dashboard: http://localhost:5000"
