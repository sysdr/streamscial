#!/bin/bash

echo "🛑 Stopping Day 34 SMT Pipeline..."

# Stop Python processes
pkill -f "python.*web_server.py" || true
pkill -f "python.*pipeline_orchestrator.py" || true
pkill -f "python.*producer.py" || true

# Stop Docker containers
docker compose down

echo "✅ All services stopped!"
