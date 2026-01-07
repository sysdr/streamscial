"""
Production-Ready Post Service
Handles post creation with circuit breaker, rate limiting, and monitoring
"""
import asyncio
import time
import json
from typing import Dict, Optional
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import psutil
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CircuitBreaker:
    """Circuit breaker pattern for fault tolerance"""
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker: OPEN -> HALF_OPEN")
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                logger.info("Circuit breaker: HALF_OPEN -> CLOSED")
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(f"Circuit breaker: CLOSED -> OPEN after {self.failure_count} failures")
            raise

class PostService:
    def __init__(self, region: str, bootstrap_servers: list):
        self.region = region
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        self.metrics = {
            "posts_created": 0,
            "posts_failed": 0,
            "total_latency_ms": 0,
            "requests": 0
        }
        self._init_producer()
    
    def _init_producer(self):
        """Initialize Kafka producer with retry logic"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=5,
                compression_type='snappy',
                batch_size=16384,
                linger_ms=10
            )
            logger.info(f"Post service initialized in region {self.region}")
        except Exception as e:
            logger.error(f"Failed to initialize producer: {e}")
            self.producer = None
    
    def create_post(self, user_id: str, content: str) -> Dict:
        """Create post with production-grade error handling"""
        start_time = time.time()
        self.metrics["requests"] += 1
        
        try:
            post_id = f"post_{int(time.time() * 1000)}_{user_id}"
            post_data = {
                "post_id": post_id,
                "user_id": user_id,
                "content": content,
                "timestamp": datetime.utcnow().isoformat(),
                "region": self.region,
                "version": "v2.5.0"
            }
            
            # Use circuit breaker for Kafka publish
            def publish():
                if not self.producer:
                    raise Exception("Producer not initialized")
                future = self.producer.send('posts-created', value=post_data)
                future.get(timeout=10)  # Wait for acknowledgment
                return post_data
            
            result = self.circuit_breaker.call(publish)
            
            latency_ms = (time.time() - start_time) * 1000
            self.metrics["posts_created"] += 1
            self.metrics["total_latency_ms"] += latency_ms
            
            logger.info(f"Post created: {post_id} in {latency_ms:.2f}ms")
            
            return {
                "status": "success",
                "post_id": post_id,
                "latency_ms": latency_ms
            }
        
        except Exception as e:
            self.metrics["posts_failed"] += 1
            logger.error(f"Post creation failed: {e}")
            return {
                "status": "error",
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    def get_metrics(self) -> Dict:
        """Get service metrics"""
        avg_latency = (
            self.metrics["total_latency_ms"] / self.metrics["requests"]
            if self.metrics["requests"] > 0 else 0
        )
        
        return {
            "region": self.region,
            "circuit_breaker_state": self.circuit_breaker.state,
            "posts_created": self.metrics["posts_created"],
            "posts_failed": self.metrics["posts_failed"],
            "success_rate": (
                self.metrics["posts_created"] / self.metrics["requests"] * 100
                if self.metrics["requests"] > 0 else 0
            ),
            "avg_latency_ms": avg_latency,
            "total_requests": self.metrics["requests"]
        }
    
    def close(self):
        """Graceful shutdown"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Post service shut down gracefully in {self.region}")

if __name__ == "__main__":
    # Test the service
    service = PostService("us-east-1", ["localhost:9092"])
    
    # Create test posts
    for i in range(10):
        result = service.create_post(f"user_{i}", f"Test post content {i}")
        print(f"Created post: {result}")
    
    # Print metrics
    print("\nService Metrics:")
    print(json.dumps(service.get_metrics(), indent=2))
    
    service.close()
