#!/bin/bash
set -e

echo "🔨 StreamSocial Schema Evolution - Build Script"
echo "============================================="

# Check Python version
echo "🐍 Checking Python version..."
if ! command -v python3.11 &> /dev/null; then
    echo "❌ Python 3.11 not found. Please install Python 3.11"
    exit 1
fi

python3.11 --version

# Create and activate virtual environment
echo "🌐 Setting up virtual environment..."
if [ ! -d "venv" ]; then
    python3.11 -m venv venv
    echo "✅ Virtual environment created"
fi

source venv/bin/activate
echo "✅ Virtual environment activated"

# Install dependencies
echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Dependencies installed"

# Verify directory structure
echo "📁 Verifying project structure..."
directories=(
    "src/producer"
    "src/consumer" 
    "src/registry"
    "src/schemas"
    "src/dashboard"
    "src/dashboard/templates"
    "src/dashboard/static/css"
    "src/dashboard/static/js"
    "tests"
    "docker"
    "scripts"
    "docs"
)

for dir in "${directories[@]}"; do
    if [ -d "$dir" ]; then
        echo "✅ $dir exists"
    else
        echo "❌ $dir missing"
        exit 1
    fi
done

# Verify key files
echo "📄 Verifying key files..."
key_files=(
    "src/schemas/user_profile_v1.json"
    "src/schemas/user_profile_v2.json"
    "src/schemas/user_profile_v3.json"
    "src/registry/schema_client.py"
    "src/producer/profile_producer.py"
    "src/consumer/profile_consumer.py"
    "src/dashboard/app.py"
    "tests/test_schema_evolution.py"
    "tests/test_integration.py"
    "docker/docker-compose.yml"
)

for file in "${key_files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
        exit 1
    fi
done

# Check code syntax
echo "🔍 Checking Python syntax..."
python -m py_compile src/registry/schema_client.py
python -m py_compile src/producer/profile_producer.py
python -m py_compile src/consumer/profile_consumer.py
python -m py_compile src/dashboard/app.py
echo "✅ Python syntax check passed"

echo ""
echo "🎉 Build completed successfully!"
echo ""
echo "Next steps:"
echo "1. Run './start.sh' to start infrastructure"
echo "2. Run 'source venv/bin/activate && python -m pytest tests/ -v' to run tests"
echo "3. Run 'source venv/bin/activate && python src/dashboard/app.py' to start dashboard"
echo ""
