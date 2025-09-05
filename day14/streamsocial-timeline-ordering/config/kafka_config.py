from dataclasses import dataclass
from typing import Dict, Any
import os

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    topic_name: str = "user-timeline"
    partitions: int = 6
    replication_factor: int = 1
    
    producer_config: Dict[str, Any] = None
    consumer_config: Dict[str, Any] = None
    
    def __post_init__(self):
        self.producer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'key_serializer': lambda x: x.encode('utf-8'),
            'value_serializer': lambda x: x.encode('utf-8'),
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 1
        }
        
        self.consumer_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'key_deserializer': lambda x: x.decode('utf-8'),
            'value_deserializer': lambda x: x.decode('utf-8'),
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
            'group_id': 'timeline-consumer-group'
        }

# Environment-based configuration
kafka_config = KafkaConfig(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
)
