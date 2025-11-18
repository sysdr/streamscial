import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
CONNECT_URL = os.getenv('CONNECT_URL', 'http://localhost:8083')

# Database Configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://streamsocial:streamsocial@localhost:5433/streamsocial')

# Redis Configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6380/0')

# Topics
TOPICS = {
    'user_profiles': 'ml-training-user_profiles',
    'user_interactions': 'ml-training-user_interactions',
    'content_metadata': 'ml-training-content_metadata',
    'features': 'ml-training-features',
    'resolved_profiles': 'ml-resolved-user_profiles',
    'resolved_content': 'ml-resolved-content_metadata'
}

# Connector Configuration
CONNECTOR_CONFIG = {
    'poll_interval_ms': 5000,
    'batch_max_rows': 1000,
    'tasks_max': 3
}

# Feature Store
FEATURE_STORE_TOPIC = 'ml-feature-store'

# Monitoring
METRICS_PORT = 8000
