"""Low-latency consumer configuration for StreamSocial notifications."""

import os

class ConsumerConfig:
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_SERVERS', 'localhost:9092')
    
    # Ultra-low latency settings
    CONSUMER_CONFIG = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'streamsocial-critical-notifications',
        'enable.auto.commit': False,
        'auto.offset.reset': 'latest',
        'fetch.min.bytes': 1,
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 2000,
        'compression.type': 'none'  # Disable compression for latency
    }
    
    # Topics
    TOPICS = ['critical-notifications', 'important-notifications', 'standard-notifications']
    
    # Performance targets
    TARGET_LATENCY_MS = 50
    MAX_PROCESSING_TIME_MS = 30
    
    # Monitoring
    METRICS_PORT = 8080
    DASHBOARD_PORT = 5000
