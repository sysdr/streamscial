import os
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    engagement_topic: str = "user_engagement_events"
    consumer_group: str = "analytics_workers"
    auto_offset_reset: str = "earliest"
    
@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    
@dataclass
class PostgresConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "streamsocial"
    username: str = "admin"
    password: str = "password123"
    
@dataclass
class AppConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    worker_id: str = os.getenv("WORKER_ID", "worker_1")
    offset_commit_interval: int = 5000  # ms
    heartbeat_interval: int = 1000  # ms
    batch_size: int = 1000
    web_port: int = 8080
