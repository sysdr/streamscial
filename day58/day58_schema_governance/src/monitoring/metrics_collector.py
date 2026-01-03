"""
Metrics Collector Service - Continuously collects and sends metrics to dashboard
Runs in background to provide real-time metrics to the governance dashboard
"""

import time
import requests
import threading
import sys
import os
from typing import Optional
import random
from io import BytesIO
import fastavro

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.registry.schema_manager import registry_client
from src.producers.schema_evolution_producer import SchemaEvolutionProducer
from src.consumers.schema_adaptive_consumer import SchemaAdaptiveConsumer
from src.monitoring.governance_monitor import GovernanceMonitor


class MetricsCollector:
    """Continuously collects metrics and sends to dashboard"""
    
    def __init__(self, dashboard_url: str = 'http://localhost:5058'):
        self.dashboard_url = dashboard_url
        self.monitor = GovernanceMonitor()
        self.producer: Optional[SchemaEvolutionProducer] = None
        self.consumer: Optional[SchemaAdaptiveConsumer] = None
        self.running = False
        self.producer_thread: Optional[threading.Thread] = None
        self.consumer_thread: Optional[threading.Thread] = None
        self.message_queue = []
        
    def setup(self):
        """Initialize producer and consumer"""
        print("Setting up metrics collector...")
        
        # Get schema directory path
        schema_dir = os.path.join(project_root, 'schemas')
        
        # Initialize producer
        self.producer = SchemaEvolutionProducer()
        self.producer.load_schemas(schema_dir)
        
        # Update schema states to APPROVED
        for version in range(1, 4):
            registry_client.update_schema_state('posts-value', version, 'APPROVED')
        
        # Initialize consumer
        self.consumer = SchemaAdaptiveConsumer()
        
        print("✓ Producer and consumer initialized")
    
    def start_producer(self):
        """Continuously produce messages with different schema versions"""
        while self.running:
            try:
                # Randomly select version based on weights
                rand = random.random()
                if rand < 0.50:
                    version = 'v1'
                elif rand < 0.85:
                    version = 'v2'
                else:
                    version = 'v3'
                
                # Create post data
                post_id = random.randint(1, 10000)
                user_id = random.randint(1, 1000)
                
                if version == 'v1':
                    post_data = self.producer.create_post_v1(post_id, user_id)
                elif version == 'v2':
                    post_data = self.producer.create_post_v2(post_id, user_id)
                else:
                    post_data = self.producer.create_post_v3(post_id, user_id)
                
                # Serialize message
                schema_info = self.producer.schemas[version]
                bytes_io = BytesIO()
                fastavro.schemaless_writer(bytes_io, schema_info['schema_obj'], post_data)
                avro_bytes = bytes_io.getvalue()
                schema_id_bytes = schema_info['id'].to_bytes(4, byteorder='big')
                message = schema_id_bytes + avro_bytes
                
                # Update producer metrics
                self.producer.metrics['sent_by_version'][version] += 1
                self.producer.metrics['total_sent'] += 1
                
                # Add to message queue for consumer
                self.message_queue.append(message)
                
                # Sleep to control production rate
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error in producer: {e}")
                time.sleep(1)
    
    def start_consumer(self):
        """Continuously consume messages"""
        while self.running:
            try:
                if self.message_queue:
                    message = self.message_queue.pop(0)
                    self.consumer.consume_messages([message])
                
                time.sleep(0.3)
                
            except Exception as e:
                print(f"Error in consumer: {e}")
                time.sleep(1)
    
    def collect_and_send_metrics(self):
        """Collect metrics and send to dashboard"""
        while self.running:
            try:
                # Collect all metrics
                report = self.monitor.generate_health_report(
                    self.producer,
                    self.consumer
                )
                
                # Send to dashboard
                try:
                    response = requests.post(
                        f'{self.dashboard_url}/api/update_metrics',
                        json=report,
                        timeout=2
                    )
                    if response.status_code == 200:
                        print(f"✓ Metrics sent to dashboard at {time.strftime('%H:%M:%S')}")
                    else:
                        print(f"✗ Failed to send metrics: {response.status_code}")
                except requests.exceptions.RequestException as e:
                    print(f"✗ Dashboard not reachable: {e}")
                
                # Wait before next collection
                time.sleep(3)
                
            except Exception as e:
                print(f"Error collecting metrics: {e}")
                time.sleep(5)
    
    def start(self):
        """Start all services"""
        if self.running:
            return
        
        print("="*70)
        print("Starting Metrics Collector Service")
        print("="*70)
        
        self.setup()
        self.running = True
        
        # Start producer thread
        self.producer_thread = threading.Thread(target=self.start_producer, daemon=True)
        self.producer_thread.start()
        print("✓ Producer thread started")
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(target=self.start_consumer, daemon=True)
        self.consumer_thread.start()
        print("✓ Consumer thread started")
        
        # Start metrics collection
        print("✓ Metrics collection started")
        print(f"📊 Dashboard: {self.dashboard_url}")
        print("="*70)
        
        # Wait a bit for threads to start producing/consuming
        time.sleep(2)
        
        # Collect and send metrics in main thread
        try:
            self.collect_and_send_metrics()
        except KeyboardInterrupt:
            self.stop()
    
    def stop(self):
        """Stop all services"""
        print("\nStopping metrics collector...")
        self.running = False
        print("✓ Metrics collector stopped")


if __name__ == '__main__':
    collector = MetricsCollector()
    try:
        collector.start()
    except KeyboardInterrupt:
        collector.stop()

