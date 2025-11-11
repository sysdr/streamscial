#!/bin/bash

echo "🔨 Building Day 36: Connect Monitoring System"

# Ensure we're in the project directory
cd "$(dirname "$0")"

# Activate virtual environment
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3.11 -m venv venv
fi

source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL..."
for i in {1..30}; do
    if pg_isready -h localhost -p 5432 -U admin > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready"
        break
    fi
    sleep 1
done

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/ -v

echo "✅ Build complete!"
