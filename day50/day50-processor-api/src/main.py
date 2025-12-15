"""
Main application - starts topology, generator, and API
"""
import sys
import time
import threading
import signal
from src.streaming_topology import ProcessorTopology
from src.processors.content_scoring_processor import (
    FeatureExtractionProcessor,
    ScoreAggregationProcessor,
    RecommendationProcessor
)
from src.utils.event_generator import EventGenerator
from src.api.server import start_api

# Global references for cleanup
topology = None
generator = None

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\n\nShutting down gracefully...")
    if generator:
        generator.stop()
    if topology:
        topology.stop()
    sys.exit(0)

def main():
    global topology, generator
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print("=" * 60)
    print("Day 50: Processor API - Content Recommendation Engine")
    print("=" * 60)
    
    # Create topology
    print("\n1. Creating processor topology...")
    topology = ProcessorTopology(bootstrap_servers='localhost:9092')
    
    # Add processors
    print("2. Adding processors to topology...")
    topology.add_processor(FeatureExtractionProcessor())
    topology.add_processor(ScoreAggregationProcessor())
    topology.add_processor(RecommendationProcessor())
    
    # Start event generator
    print("\n3. Starting event generator...")
    generator = EventGenerator(bootstrap_servers='localhost:9092')
    generator.start(target_rate=1000)  # 1000 events/sec for demo
    
    # Wait for topics to be created
    print("Waiting for topics to be ready...")
    time.sleep(5)
    
    # Start API server in background
    print("\n4. Starting REST API server...")
    api_thread = threading.Thread(
        target=start_api,
        args=(topology,),
        kwargs={'host': '0.0.0.0', 'port': 8080},
        daemon=True
    )
    api_thread.start()
    
    # Start topology
    print("\n5. Starting processor topology...")
    input_topics = ['user-engagement', 'content-metadata', 'user-preferences']
    
    # Run topology in background
    topology_thread = threading.Thread(
        target=topology.start,
        args=(input_topics,),
        daemon=True
    )
    topology_thread.start()
    
    print("\n" + "=" * 60)
    print("System running!")
    print("=" * 60)
    print(f"\nAPI Endpoints:")
    print(f"  - Health: http://localhost:8080/api/health")
    print(f"  - Metrics: http://localhost:8080/api/metrics")
    print(f"  - Recommendations: http://localhost:8080/api/recommendations/<user_id>")
    print(f"  - State Store Info: http://localhost:8080/api/state/<store_name>")
    print(f"\nDashboard: http://localhost:8000")
    print(f"\nPress Ctrl+C to stop\n")
    
    # Periodic metrics display
    try:
        while True:
            time.sleep(10)
            metrics = topology.get_metrics()
            gen_metrics = generator.get_metrics()
            
            print(f"\n[Metrics @ {time.strftime('%H:%M:%S')}]")
            print(f"  Events Generated: {gen_metrics['events_sent']}")
            print(f"  Events Processed: {metrics['events_processed']}")
            print(f"  Throughput: {metrics['throughput_per_sec']} events/sec")
            print(f"  Latency p50: {metrics['latency_p50']:.2f}ms")
            print(f"  Latency p99: {metrics['latency_p99']:.2f}ms")
            print(f"  Errors: {metrics['errors']}")
            
            for store_name, store_info in metrics['state_stores'].items():
                print(f"  {store_name}: {store_info['size']} entries, {store_info['metrics']['writes']} writes")
    
    except KeyboardInterrupt:
        signal_handler(None, None)

if __name__ == '__main__':
    main()
