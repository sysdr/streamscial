#!/bin/bash
echo "🛑 Stopping StreamSocial services..."
pkill -f "python src/dashboard.py"
echo "Services stopped."
