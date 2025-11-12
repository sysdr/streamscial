#!/bin/bash

# Stop the connector

echo "Stopping SocialStream Connector..."

# Find and kill process
pkill -f "python src/main.py" || true

echo "Connector stopped"
