#!/bin/bash
set -e

echo "🔧 Building StreamSocial Geo-Partitioner..."

# Create and activate virtual environment
if [ ! -d "venv" ]; then
    python3.11 -m venv venv
fi
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Compile Java code
echo "☕ Compiling Java partitioner..."
mkdir -p build/classes
javac -cp "/usr/share/kafka/libs/*" -d build/classes src/main/java/com/streamsocial/**/*.java

# Create JAR
echo "📦 Creating JAR file..."
cd build/classes
jar cf ../../streamsocial-geo-partitioner.jar com/
cd ../..

echo "✅ Build completed successfully!"
echo "📋 Next steps:"
echo "   1. Run './start.sh' to start all services"
echo "   2. Run tests with 'pytest test_geo_partitioner.py'"
echo "   3. Open http://localhost:5000 for monitoring dashboard"
