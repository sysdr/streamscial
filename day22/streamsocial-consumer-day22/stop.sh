#!/bin/bash

echo "🛑 Stopping StreamSocial services..."

cd docker
docker-compose down

echo "✅ All services stopped"
