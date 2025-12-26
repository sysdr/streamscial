#!/bin/bash
set -e

echo "Building Day 57 Microservices Architecture..."

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install fastapi==0.109.0 uvicorn==0.27.0 kafka-python==2.3.0 \
            psycopg2-binary==2.9.9 flask==3.0.0 flask-cors==4.0.0 \
            requests==2.31.0 pytest==7.4.4 pydantic==2.5.3 six

echo "✓ Build completed successfully"
