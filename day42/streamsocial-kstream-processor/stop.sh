#!/bin/bash

echo "=== Stopping StreamSocial KStream Processor ==="

# Stop Docker containers
docker-compose down

echo "Stopped!"
