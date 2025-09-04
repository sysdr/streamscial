import os

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'client_id': 'streamsocial-producer',
    'retries': 2147483647,  # MAX_INT
    'acks': 'all',
    'max_in_flight_requests_per_connection': 5,
    'compression_type': 'gzip',
    'batch_size': 16384,
    'linger_ms': 5
}

TOPICS = {
    'posts': 'streamsocial-posts',
    'metrics': 'streamsocial-metrics'
}

CHAOS_CONFIG = {
    'failure_rate': 0.3,  # 30% chance of network failure
    'min_failure_duration': 0.1,  # 100ms
    'max_failure_duration': 2.0   # 2 seconds
}
