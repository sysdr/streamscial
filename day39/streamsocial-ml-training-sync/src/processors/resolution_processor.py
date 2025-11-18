import json
from typing import Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError
from src.resolvers.conflict_resolver import ConflictResolver
import structlog
import time

logger = structlog.get_logger()

class ResolutionProcessor:
    """Processes records through conflict resolution"""
    
    def __init__(self, bootstrap_servers: str, group_id: str = 'resolution-processor'):
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all'
        }
        self.resolver = ConflictResolver()
        self.state: Dict[str, Dict[str, Any]] = {}
        self.running = False
    
    def process_stream(self,
                       input_topic: str,
                       output_topic: str,
                       record_type: str,
                       key_field: str):
        """Process stream with conflict resolution"""
        
        consumer = Consumer(self.consumer_config)
        producer = Producer(self.producer_config)
        
        consumer.subscribe([input_topic])
        self.running = True
        
        try:
            while self.running:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("consumer_error", error=msg.error())
                    continue
                
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    
                    # Handle schema-wrapped values
                    if 'payload' in value:
                        value = value['payload']
                    
                    key = str(value.get(key_field))
                    
                    # Resolve conflict
                    existing = self.state.get(key)
                    if existing:
                        resolved = self.resolver.resolve(record_type, existing, value)
                    else:
                        resolved = value
                    
                    # Update state and produce
                    self.state[key] = resolved
                    
                    producer.produce(
                        output_topic,
                        key=key.encode('utf-8'),
                        value=json.dumps(resolved).encode('utf-8')
                    )
                    
                    consumer.commit()
                    
                except json.JSONDecodeError as e:
                    logger.error("json_decode_error", error=str(e))
                except Exception as e:
                    logger.error("processing_error", error=str(e))
                    
        finally:
            consumer.close()
            producer.flush()
    
    def stop(self):
        self.running = False
