#!/bin/bash

echo "🎬 Running StreamSocial Kafka Batching Demo..."

source venv/bin/activate

# Start services
./start.sh
sleep 15

echo "🔧 Running configuration comparison demo..."
python << 'EOF'
import asyncio
import time
from src.producer.adaptive_producer import AdaptiveKafkaProducer
from src.utils.load_simulator import LoadSimulator

async def demo_configurations():
    configs = ['low_latency', 'high_throughput', 'adaptive']
    
    for config in configs:
        print(f"\n🔍 Testing {config} configuration...")
        
        producer = AdaptiveKafkaProducer(config)
        producer.start_monitoring()
        simulator = LoadSimulator(producer)
        
        # Run benchmark
        results = await simulator.benchmark_throughput(10000, 30)
        
        print(f"   Target: {results['target_rate']} msg/s")
        print(f"   Actual: {results['actual_rate']:.1f} msg/s")
        print(f"   Success: {results['success_rate']:.1f}%")
        
        producer.close()
        await asyncio.sleep(5)

asyncio.run(demo_configurations())
