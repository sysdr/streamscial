import json
import redis
from typing import Optional
from src.models.payment import PaymentResult
import structlog

logger = structlog.get_logger(__name__)

class IdempotencyService:
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, ttl_hours: int = 24):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.ttl_seconds = ttl_hours * 3600
        
    def is_processed(self, idempotency_key: str) -> bool:
        """Check if a payment has already been processed"""
        try:
            return self.redis_client.exists(f"payment:{idempotency_key}") > 0
        except Exception as e:
            logger.error("Failed to check idempotency", key=idempotency_key, error=str(e))
            return False
    
    def store_result(self, idempotency_key: str, result: PaymentResult):
        """Store payment result for idempotency checking"""
        try:
            key = f"payment:{idempotency_key}"
            value = result.model_dump_json()
            self.redis_client.setex(key, self.ttl_seconds, value)
            logger.debug("Stored payment result", key=idempotency_key)
        except Exception as e:
            logger.error("Failed to store payment result", key=idempotency_key, error=str(e))
    
    def get_result(self, idempotency_key: str) -> Optional[PaymentResult]:
        """Get stored payment result"""
        try:
            key = f"payment:{idempotency_key}"
            value = self.redis_client.get(key)
            if value:
                return PaymentResult(**json.loads(value))
            return None
        except Exception as e:
            logger.error("Failed to get payment result", key=idempotency_key, error=str(e))
            return None
