#!/bin/bash

echo "========================================="
echo "Day 45 KTable Operations Demo"
echo "========================================="
echo ""
echo "Services:"
echo "  Dashboard:  http://localhost:8002"
echo "  Query API:  http://localhost:8001/docs"
echo ""
echo "Example API queries:"
echo "  curl http://localhost:8001/api/reputation/user_00001"
echo "  curl http://localhost:8001/api/top-users?limit=10"
echo "  curl http://localhost:8001/api/stats"
echo ""
echo "Monitor logs:"
echo "  Stream Processor: Processing events and updating KTable"
echo "  Event Producer: Generating user actions"
echo ""
echo "Press Ctrl+C to stop demo"
echo ""

tail -f /dev/null

