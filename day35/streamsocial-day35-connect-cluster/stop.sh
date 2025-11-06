#!/bin/bash

echo "Stopping StreamSocial Connect Cluster..."

# Stop Docker services
docker compose down -v

echo "Cleanup complete!"
