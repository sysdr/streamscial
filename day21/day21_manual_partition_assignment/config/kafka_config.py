import os
from typing import Dict, List

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    SOCIAL_POSTS_TOPIC = 'social-posts'
    TRENDING_RESULTS_TOPIC = 'trending-results'
    CONSUMER_GROUP_ID = 'trend-workers'
    PARTITION_COUNT = 6
    REPLICATION_FACTOR = 1
    
    # Manual Assignment Strategy
    WORKER_COUNT = 3
    PARTITIONS_PER_WORKER = PARTITION_COUNT // WORKER_COUNT
    
    # State Management
    STATE_CHECKPOINT_INTERVAL = 30  # seconds
    STATE_RECOVERY_TIMEOUT = 60  # seconds
    
    # Trend Detection
    TREND_WINDOW_SIZE = 300  # 5 minutes
    MIN_HASHTAG_COUNT = 3  # Reduced threshold for demo
    TREND_UPDATE_INTERVAL = 5  # seconds
    
    # Kafka Timeout Settings
    CONSUMER_TIMEOUT_MS = 1000  # 1 second - shorter timeout for more responsive polling
    PRODUCER_TIMEOUT_MS = 5000  # 5 seconds
    REQUEST_TIMEOUT_MS = 35000  # 35 seconds - must be larger than session timeout
    SESSION_TIMEOUT_MS = 30000  # 30 seconds
    HEARTBEAT_INTERVAL_MS = 10000  # 10 seconds

class RedisConfig:
    HOST = os.getenv('REDIS_HOST', 'localhost')
    PORT = int(os.getenv('REDIS_PORT', 6379))
    STATE_DB = 0
    METRICS_DB = 1
