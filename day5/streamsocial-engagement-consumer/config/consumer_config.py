import os
from typing import Dict, Any

class ConsumerConfig:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'engagement-consumer-group-v2')
    KAFKA_TOPICS = ['user-engagements']
    
    # Consumer settings - Fixed to avoid rebalancing issues
    AUTO_OFFSET_RESET = 'earliest'  # Start from beginning to ensure we process all messages
    ENABLE_AUTO_COMMIT = True  # Enable auto-commit to avoid manual commit issues
    MAX_POLL_RECORDS = 100  # Reduce batch size to avoid long processing times
    SESSION_TIMEOUT_MS = 60000  # Increase session timeout
    HEARTBEAT_INTERVAL_MS = 20000  # Increase heartbeat interval
    MAX_POLL_INTERVAL_MS = 300000  # Increase max poll interval to avoid rebalancing
    
    # Processing settings
    BATCH_SIZE = 100
    MAX_RETRIES = 3
    RETRY_BACKOFF_MS = 1000
    
    # Redis settings
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB = int(os.getenv('REDIS_DB', '0'))
    
    # Web dashboard
    WEB_HOST = os.getenv('WEB_HOST', '0.0.0.0')
    WEB_PORT = int(os.getenv('WEB_PORT', '5000'))

    @classmethod
    def get_kafka_config(cls) -> Dict[str, Any]:
        return {
            'bootstrap_servers': cls.KAFKA_BOOTSTRAP_SERVERS.split(','),
            'group_id': cls.KAFKA_GROUP_ID,
            'auto_offset_reset': cls.AUTO_OFFSET_RESET,
            'enable_auto_commit': cls.ENABLE_AUTO_COMMIT,
            'max_poll_records': cls.MAX_POLL_RECORDS,
            'session_timeout_ms': cls.SESSION_TIMEOUT_MS,
            'heartbeat_interval_ms': cls.HEARTBEAT_INTERVAL_MS,
            'max_poll_interval_ms': cls.MAX_POLL_INTERVAL_MS,
        }
