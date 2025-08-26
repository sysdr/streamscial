import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_SERVERS', 'localhost:9092')
KAFKA_TOPICS = {
    'user_interactions': 'user-interactions',
    'feed_requests': 'feed-requests',
    'system_metrics': 'system-metrics'
}

# Consumer Group Configuration
CONSUMER_GROUP_ID = 'streamsocial-feed-generators'
PARTITION_COUNT = 6
REPLICATION_FACTOR = 1

# Auto-scaling Configuration
LAG_THRESHOLD = 1000
MAX_CONSUMERS = 12
MIN_CONSUMERS = 3
SCALE_UP_COOLDOWN = 30  # seconds
SCALE_DOWN_COOLDOWN = 60  # seconds

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
CACHE_TTL = 300  # 5 minutes

# Monitoring Configuration
PROMETHEUS_PORT = 8000
DASHBOARD_PORT = 8050
