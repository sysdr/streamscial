"""Kafka configuration for StreamSocial transactional producers."""

import os
from typing import Dict, Any

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092,localhost:9093,localhost:9094')
    
    # Producer configurations
    PRODUCER_CONFIG: Dict[str, Any] = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'enable.idempotence': True,
        'acks': 'all',
        'retries': 2147483647,  # Max int value
        'max.in.flight.requests.per.connection': 5,
        'batch.size': 16384,
        'linger.ms': 10,
        'compression.type': 'snappy',
        'request.timeout.ms': 30000,
        'delivery.timeout.ms': 120000,
        'message.timeout.ms': 30000,  # Must be <= transaction.timeout.ms
    }
    
    # Consumer configurations  
    CONSUMER_CONFIG: Dict[str, Any] = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'streamsocial-consumers',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
        'isolation.level': 'read_committed',  # Only read committed transactions
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 300000,
    }
    
    # Topics
    TOPICS = {
        'user_timeline': 'user-timeline',
        'global_feed': 'global-feed', 
        'notifications': 'notifications',
        'analytics': 'analytics',
        'moderation': 'content-moderation'
    }
    
    # Transaction settings
    TRANSACTION_TIMEOUT_MS = 60000
    MAX_POLL_RECORDS = 500
