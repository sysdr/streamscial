"""
StreamSocial Day 35: Distributed Connect Cluster
Main application orchestrator
"""
import sys
import time
import signal
from monitoring.dashboard_server import start_dashboard
from connectors.connector_manager import ConnectorManager, deploy_streamsocial_connectors
from utils.data_generator import DataGenerator


def signal_handler(sig, frame):
    print("\nShutting down gracefully...")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    print("=" * 60)
    print("StreamSocial - Day 35: Distributed Connect Deployment")
    print("Fault-Tolerant Content Ingestion Pipeline")
    print("=" * 60)
    
    # Wait for Connect cluster to be ready
    print("\nWaiting for Connect cluster to be ready...")
    time.sleep(20)
    
    # Deploy connectors
    print("\nDeploying StreamSocial connectors...")
    manager = ConnectorManager("http://localhost:8083")
    
    try:
        results = deploy_streamsocial_connectors(manager)
        print("\nConnector deployment results:")
        for result in results:
            if 'error' in result:
                print(f"  ❌ Error: {result['error']}")
            else:
                print(f"  ✓ {result.get('name', 'Unknown')} deployed successfully")
    except Exception as e:
        print(f"Error deploying connectors: {e}")
    
    # Start data generator
    print("\nStarting continuous data generation...")
    generator = DataGenerator({
        'host': 'localhost',
        'port': 5433,
        'database': 'streamsocial',
        'user': 'admin',
        'password': 'admin123'
    })
    generator.start_continuous_generation(interval=5)
    
    # Start dashboard
    print("\n" + "=" * 60)
    print("Dashboard URL: http://localhost:5000")
    print("=" * 60)
    print("\nMonitoring cluster... Press Ctrl+C to stop\n")
    
    start_dashboard()


if __name__ == '__main__':
    main()
