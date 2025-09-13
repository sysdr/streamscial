#!/bin/bash

echo "🎬 StreamSocial Compression Demo"
echo "==============================="

source venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Start services
echo "Starting demo services..."
./start.sh

sleep 3

# Run automated tests
echo ""
echo "🧪 Running automated compression tests..."
cd src

python3 << 'DEMO_SCRIPT'
import time
import json
from compression.analyzer import CompressionAnalyzer
from compression.streamsocial_data import StreamSocialDataGenerator

print("\n🔬 StreamSocial Compression Analysis Demo")
print("=" * 45)

analyzer = CompressionAnalyzer()
generator = StreamSocialDataGenerator()

# Test different data types
data_types = ["user_profile", "post_metadata", "timeline_update"]
algorithms = ["gzip", "snappy", "lz4", "zstd"]

for data_type in data_types:
    print(f"\n📊 Testing {data_type} data:")
    print("-" * 30)
    
    # Generate test data
    test_data = generator.generate_data(data_type, 100)
    print(f"Generated {len(test_data):,} bytes of {data_type} data")
    
    # Test each algorithm
    for algorithm in algorithms:
        try:
            result = analyzer.benchmark_algorithm(test_data, algorithm, iterations=10)
            print(f"{algorithm:>6}: {result['compression_ratio']:.2f}x ratio, "
                  f"{result['compression_time_ms']['mean']:.2f}ms compress, "
                  f"{result['space_savings_percent']:.1f}% space saved")
        except Exception as e:
            print(f"{algorithm:>6}: Error - {str(e)}")

print(f"\n🎯 Demo completed! Visit http://localhost:8080 for interactive analysis")
print("💡 Try different combinations of data types and algorithms in the dashboard")
DEMO_SCRIPT

cd ..
echo ""
echo "✅ Demo completed successfully!"
echo "🌐 Dashboard is running at: http://localhost:8080"
echo "📝 Check the web interface for interactive compression analysis"
