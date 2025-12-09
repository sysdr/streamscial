import sys
import threading
import time
from streaming.trending_processor import TrendingProcessor
from api.query_api import InteractiveQueryAPI

def main():
    instance_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    port = 8080 + instance_id
    
    # Create processor
    processor = TrendingProcessor(
        application_id='trending-topics',
        bootstrap_servers='localhost:9092',
        instance_id=instance_id,
        port=port
    )
    
    # Define peer instances (other instances in cluster)
    peer_instances = []
    if instance_id == 0:
        peer_instances = [{'host': 'localhost', 'port': 8081}]
    else:
        peer_instances = [{'host': 'localhost', 'port': 8080}]
    
    # Create API
    api = InteractiveQueryAPI(processor, peer_instances)
    
    # Start processor in separate thread
    processor_thread = threading.Thread(target=processor.process, daemon=True)
    processor_thread.start()
    
    # Give processor time to initialize
    time.sleep(2)
    
    # Start API server
    print(f"Starting API server on port {port}...")
    print(f"Dashboard: http://localhost:{port}/web/dashboard.html")
    api.run(port=port)

if __name__ == '__main__':
    main()
