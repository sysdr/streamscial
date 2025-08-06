"""Kafka configuration for StreamSocial partitioning strategy"""

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092', 'localhost:9093', 'localhost:9094'],
    'api_version': (2, 8, 1),
    'request_timeout_ms': 30000,
    'metadata_max_age_ms': 300000
}

ADMIN_CONFIG = {
    'bootstrap_servers': ['localhost:9092', 'localhost:9093', 'localhost:9094'],
    'api_version': (2, 8, 1),
    'request_timeout_ms': 30000
}

TOPIC_CONFIG = {
    'user-actions': {
        'partitions': 1000,
        'replication_factor': 3,
        'partition_key_strategy': 'user_id_hash',
        'ordering_guarantee': 'strict'
    },
    'content-interactions': {
        'partitions': 500,
        'replication_factor': 3,
        'partition_key_strategy': 'content_id_hash',
        'ordering_guarantee': 'relaxed'
    }
}

CONSUMER_CONFIG = {
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 1000,
    'session_timeout_ms': 30000,
    'heartbeat_interval_ms': 3000,
    'max_poll_records': 500,
    'max_poll_interval_ms': 300000
}

PRODUCER_CONFIG = {
    'acks': 'all',
    'compression_type': 'snappy',
    'batch_size': 16384,
    'linger_ms': 10,
    'buffer_memory': 33554432,
    'max_request_size': 1048576,
    'retries': 3
}
