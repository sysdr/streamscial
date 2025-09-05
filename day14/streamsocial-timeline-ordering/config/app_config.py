from pydantic_settings import BaseSettings
from typing import List

class AppSettings(BaseSettings):
    app_name: str = "StreamSocial Timeline Ordering"
    debug: bool = True
    host: str = "0.0.0.0"
    port: int = 8000
    
    # Kafka settings
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "user-timeline"
    
    # Timeline settings
    max_timeline_posts: int = 100
    timeline_refresh_interval: int = 5
    
    class Config:
        env_file = ".env"
        extra = "ignore"  # Ignore extra environment variables

settings = AppSettings()
