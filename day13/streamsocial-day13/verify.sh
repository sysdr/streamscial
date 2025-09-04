#!/bin/bash

echo "🔍 Verifying StreamSocial setup..."

# Check directory structure
echo "📁 Checking directory structure..."
directories=("src" "tests" "config" "templates" "docker" "static/css" "static/js")
for dir in "${directories[@]}"; do
    if [ -d "$dir" ]; then
        echo "✅ $dir exists"
    else
        echo "❌ $dir missing"
    fi
done

# Check files
echo "📄 Checking key files..."
files=("src/idempotent_producer.py" "src/duplicate_detector.py" "src/dashboard.py" "templates/dashboard.html" "config/kafka_config.py" "requirements.txt")
for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

# Check if virtual environment can be created
echo "🐍 Testing Python 3.11 availability..."
if command -v python3.11 &> /dev/null; then
    echo "✅ Python 3.11 available"
else
    echo "❌ Python 3.11 not found"
fi

echo "🎯 Setup verification complete!"
echo "Run './start.sh' to start the system"
