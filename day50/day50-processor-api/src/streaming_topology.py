"""
Kafka Streaming Topology using Processor API
Wires together the custom processors
"""
import json
import time
import threading
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, List
from src.processors.content_scoring_processor import (
    FeatureExtractionProcessor,
    ScoreAggregationProcessor,
    RecommendationProcessor,
    ProcessorContext,
    StateStore
)

class ProcessorTopology:
    """Manages processor chain and state stores"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        
        # State stores
        self.state_stores = {
            'feature-store': StateStore('feature-store', 'feature-store-changelog'),
            'score-store': StateStore('score-store', 'score-store-changelog'),
            'recommendation-store': StateStore('recommendation-store', 'recommendation-store-changelog')
        }
        
        # Processors
        self.processors = []
        self.context = ProcessorContext(self.state_stores)
        
        # Kafka clients
        self.consumer = None
        self.producer = None
        self.running = False
        
        # Metrics
        self.metrics = {
            'events_processed': 0,
            'processing_latency_ms': [],
            'throughput_per_sec': 0,
            'errors': 0
        }
        
        # Threading
        self.lock = threading.Lock()
        
    def add_processor(self, processor):
        """Add processor to topology"""
        self.processors.append(processor)
        processor.init(self.context)
        
    def start(self, input_topics: List[str], output_topic: str = None):
        """Start processing"""
        print(f"Starting topology with {len(self.processors)} processors...")
        
        # Initialize Kafka clients
        self.consumer = KafkaConsumer(
            *input_topics,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='processor-topology',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        if output_topic:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        
        self.running = True
        
        # Start processing loop
        self._process_loop()
    
    def _process_loop(self):
        """Main processing loop"""
        print("Processing loop started...")
        last_throughput_check = time.time()
        events_since_check = 0
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                start_time = time.time()
                
                # Process through chain
                key = message.key.decode('utf-8') if message.key else 'null'
                value = message.value
                
                result = self._process_chain(key, value)
                
                # Track latency
                latency_ms = (time.time() - start_time) * 1000
                with self.lock:
                    self.metrics['events_processed'] += 1
                    self.metrics['processing_latency_ms'].append(latency_ms)
                    if len(self.metrics['processing_latency_ms']) > 1000:
                        self.metrics['processing_latency_ms'] = self.metrics['processing_latency_ms'][-1000:]
                
                events_since_check += 1
                
                # Calculate throughput
                if time.time() - last_throughput_check >= 1.0:
                    with self.lock:
                        self.metrics['throughput_per_sec'] = events_since_check
                    events_since_check = 0
                    last_throughput_check = time.time()
                
                # Commit periodically
                if self.metrics['events_processed'] % 100 == 0:
                    self.consumer.commit()
                    
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self.stop()
    
    def _process_chain(self, key: str, value: Dict):
        """Process event through processor chain"""
        try:
            current_value = value
            for processor in self.processors:
                result = processor.process(key, current_value)
                if result:
                    _, current_value = result
                else:
                    break
            return current_value
        except Exception as e:
            with self.lock:
                self.metrics['errors'] += 1
            print(f"Error in processing chain: {e}")
            return None
    
    def stop(self):
        """Stop topology"""
        print("Stopping topology...")
        self.running = False
        
        # Close processors
        for processor in self.processors:
            processor.close()
        
        # Close Kafka clients
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        
        print("Topology stopped")
    
    def get_metrics(self) -> Dict:
        """Get topology metrics"""
        with self.lock:
            latencies = self.metrics['processing_latency_ms']
            
            # Get processor metrics
            processor_metrics = {}
            for i, processor in enumerate(self.processors):
                processor_name = processor.__class__.__name__
                processor_metrics[processor_name] = {
                    'processed': processor.metrics.get('processed', 0),
                    'errors': processor.metrics.get('errors', 0)
                }
            
            return {
                'events_processed': self.metrics['events_processed'],
                'throughput_per_sec': self.metrics['throughput_per_sec'],
                'latency_p50': sorted(latencies)[len(latencies)//2] if latencies else 0,
                'latency_p99': sorted(latencies)[int(len(latencies)*0.99)] if latencies else 0,
                'errors': self.metrics['errors'],
                'processors': processor_metrics,
                'state_stores': {
                    name: {
                        'size': store.size(),
                        'metrics': store.get_metrics()
                    }
                    for name, store in self.state_stores.items()
                }
            }
    
    def get_recommendations(self, user_id: str) -> Dict:
        """Query recommendations for a user"""
        rec_store = self.state_stores['recommendation-store']
        return rec_store.get(user_id)
