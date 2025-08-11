import redis
import json
import structlog
from typing import Optional
from config.consumer_config import ConsumerConfig
from src.models.engagement import EngagementStats

logger = structlog.get_logger()

class CacheManager:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=ConsumerConfig.REDIS_HOST,
            port=ConsumerConfig.REDIS_PORT,
            db=ConsumerConfig.REDIS_DB,
            decode_responses=True
        )
        logger.info("Connected to Redis cache")
    
    def update_engagement_stats(self, post_id: str, action_type: str):
        """Update engagement statistics in cache"""
        key = f"engagement:{post_id}"
        
        try:
            # Get existing stats or create new
            existing_data = self.redis_client.get(key)
            if existing_data:
                stats_data = json.loads(existing_data)
                stats = EngagementStats(**stats_data)
            else:
                from datetime import datetime
                stats = EngagementStats(post_id=post_id, last_updated=datetime.now())
            
            # Update stats
            stats.update(action_type)
            
            # Store back to Redis
            self.redis_client.set(
                key, 
                stats.model_dump_json(),
                ex=86400  # 24 hours expiry
            )
            
            logger.debug("Updated engagement stats", post_id=post_id, action_type=action_type)
            return stats
            
        except Exception as e:
            logger.error("Failed to update engagement stats", error=str(e), post_id=post_id)
            return None
    
    def get_engagement_stats(self, post_id: str) -> Optional[EngagementStats]:
        """Get engagement statistics from cache"""
        key = f"engagement:{post_id}"
        
        try:
            data = self.redis_client.get(key)
            if data:
                return EngagementStats(**json.loads(data))
            return None
        except Exception as e:
            logger.error("Failed to get engagement stats", error=str(e), post_id=post_id)
            return None
    
    def get_all_engagement_stats(self):
        """Get all engagement statistics"""
        try:
            keys = self.redis_client.keys("engagement:*")
            stats = {}
            for key in keys:
                data = self.redis_client.get(key)
                if data:
                    post_id = key.replace("engagement:", "")
                    stats[post_id] = json.loads(data)
            return stats
        except Exception as e:
            logger.error("Failed to get all engagement stats", error=str(e))
            return {}
