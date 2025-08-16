import os
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class KafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    topics: Dict[str, int] = None
    consumer_groups: Dict[str, str] = None
    
    def __post_init__(self):
        if self.topics is None:
            self.topics = {
                "user-activities": 12,
                "generated-feeds": 8,
                "consumer-metrics": 4
            }
        
        if self.consumer_groups is None:
            self.consumer_groups = {
                "feed-generation-workers": "feed-generation-workers",
                "feed-cache-builders": "feed-cache-builders",
                "metrics-collectors": "metrics-collectors"
            }

@dataclass 
class ConsumerConfig:
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 100
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000
    
config = KafkaConfig()
