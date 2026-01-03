#!/bin/bash

echo "=========================================="
echo "Stopping Schema Governance Framework"
echo "=========================================="

# Kill dashboard processes
pkill -f governance_dashboard.py

echo "✓ All services stopped"
