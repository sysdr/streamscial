import os
import json

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'client_id': 'streamsocial-retry-client',
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'key_serializer': lambda x: x.encode('utf-8') if x else None,
    'acks': 'all',
    'retries': 0,  # We handle retries manually
    'max_in_flight_requests_per_connection': 1,
    'compression_type': 'snappy',
    'batch_size': 16384,
    'linger_ms': 10,
    'buffer_memory': 33554432
}

RETRY_CONFIG = {
    'max_retries': 5,
    'base_delay': 100,  # milliseconds
    'max_delay': 30000,  # 30 seconds
    'backoff_multiplier': 2.0,
    'jitter_enabled': True,
    'jitter_factor': 0.25
}

CIRCUIT_BREAKER_CONFIG = {
    'failure_threshold': 5,
    'recovery_timeout': 30,
    'expected_exception': Exception
}
