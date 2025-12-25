#!/bin/bash
echo "Stopping Day 56 infrastructure..."
docker-compose down -v
sudo rm -rf /tmp/day56
echo "Stopped!"
