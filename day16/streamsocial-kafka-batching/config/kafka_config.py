import os
from typing import Dict, Any

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Batching configurations for different scenarios
    CONFIGS = {
        'low_latency': {
            'batch_size': 16384,  # 16KB
            'linger_ms': 0,       # Immediate send
            'buffer_memory': 33554432,  # 32MB
            'acks': '1',
            'compression_type': 'none',
            'retries': 0
        },
        'high_throughput': {
            'batch_size': 2097152,  # 2MB
            'linger_ms': 10,        # 10ms wait
            'buffer_memory': 2147483648,  # 2GB
            'acks': '1',
            'compression_type': 'gzip',
            'retries': 3
        },
        'peak_traffic': {
            'batch_size': 4194304,  # 4MB
            'linger_ms': 50,        # 50ms wait
            'buffer_memory': 4294967296,  # 4GB
            'acks': '1',
            'compression_type': 'gzip',
            'retries': 5
        },
        'adaptive': {
            'batch_size': 1048576,  # 1MB - starting point
            'linger_ms': 5,         # 5ms - starting point
            'buffer_memory': 1073741824,  # 1GB
            'acks': '1',
            'compression_type': 'gzip',
            'retries': 3
        }
    }
    
    TOPICS = {
        'posts': 'streamsocial.posts',
        'metrics': 'streamsocial.metrics',
        'events': 'streamsocial.events'
    }
