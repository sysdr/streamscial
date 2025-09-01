import os
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class ProducerConfig:
    bootstrap_servers: str
    acks: str
    retries: int
    batch_size: int
    linger_ms: int
    max_in_flight_requests: int
    enable_idempotence: bool
    compression_type: str

class AppConfig:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Critical Events Producer (acks=all)
    CRITICAL_PRODUCER = ProducerConfig(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks='all',
        retries=3,
        batch_size=1024,
        linger_ms=100,
        max_in_flight_requests=1,
        enable_idempotence=True,
        compression_type='gzip'
    )
    
    # Social Events Producer (acks=1)
    SOCIAL_PRODUCER = ProducerConfig(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks='1',
        retries=1,
        batch_size=16384,
        linger_ms=10,
        max_in_flight_requests=5,
        enable_idempotence=False,
        compression_type='snappy'
    )
    
    # Analytics Events Producer (acks=0)
    ANALYTICS_PRODUCER = ProducerConfig(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks='0',
        retries=0,
        batch_size=65536,
        linger_ms=50,
        max_in_flight_requests=10,
        enable_idempotence=False,
        compression_type='snappy'
    )
    
    TOPICS = {
        'critical': 'streamsocial-critical-events',
        'social': 'streamsocial-social-events',
        'analytics': 'streamsocial-analytics-events'
    }
