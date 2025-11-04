"""
Configuration management for StreamSocial sink connector
"""
import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load .env file but don't override existing environment variables
# This ensures Docker environment variables take precedence
load_dotenv(override=False)

@dataclass
class SinkConnectorConfig:
    """Configuration for hashtag sink connector"""
    
    # Kafka Configuration
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_group_id: Optional[str] = None
    
    # Database Configuration
    database_url: Optional[str] = None
    
    # Connector Configuration  
    batch_size: Optional[int] = None
    flush_timeout_ms: Optional[int] = None
    retry_backoff_ms: Optional[int] = None
    max_retries: Optional[int] = None
    
    # Monitoring Configuration
    metrics_port: Optional[int] = None
    log_level: Optional[str] = None
    
    def __post_init__(self):
        """Initialize values from environment variables after instance creation"""
        # Kafka Configuration
        if self.kafka_bootstrap_servers is None:
            self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        if self.kafka_topic is None:
            self.kafka_topic = os.getenv('KAFKA_TOPIC', 'trending-hashtags')
        if self.kafka_group_id is None:
            self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'hashtag-sink-connector')
        
        # Database Configuration
        if self.database_url is None:
            self.database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/streamsocial')
        
        # Connector Configuration
        if self.batch_size is None:
            self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        if self.flush_timeout_ms is None:
            self.flush_timeout_ms = int(os.getenv('FLUSH_TIMEOUT_MS', '30000'))
        if self.retry_backoff_ms is None:
            self.retry_backoff_ms = int(os.getenv('RETRY_BACKOFF_MS', '1000'))
        if self.max_retries is None:
            self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        
        # Monitoring Configuration
        if self.metrics_port is None:
            self.metrics_port = int(os.getenv('METRICS_PORT', '8080'))
        if self.log_level is None:
            self.log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    @classmethod
    def from_env(cls) -> 'SinkConnectorConfig':
        """Create configuration from environment variables"""
        return cls()
        
    def validate(self) -> bool:
        """Validate configuration parameters"""
        if not self.kafka_bootstrap_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        if not self.database_url:
            raise ValueError("DATABASE_URL is required")
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be positive")
        return True
