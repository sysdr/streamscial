from kafka import KafkaProducer
import json
import time
import random
import threading
import uuid
from typing import Dict, Any, Optional
import logging
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self):
        self.messages_sent = Counter('messages_sent_total', 'Total messages sent')
        self.messages_acked = Counter('messages_acked_total', 'Total messages acknowledged')
        self.duplicates_prevented = Counter('duplicates_prevented_total', 'Duplicates prevented by idempotence')
        self.send_latency = Histogram('send_latency_seconds', 'Message send latency')
        self.producer_state = Gauge('producer_state', 'Producer state (0=uninit, 1=ready, 2=error)')
        self.retries = Counter('retries_total', 'Total retry attempts')

class ChaosMonkey:
    def __init__(self, failure_rate: float = 0.3):
        self.failure_rate = failure_rate
        self.active_failures = set()
        
    def should_fail(self) -> bool:
        return random.random() < self.failure_rate
        
    def simulate_network_failure(self, duration: float):
        """Simulate network failure by introducing delay"""
        if duration > 0:
            time.sleep(duration)

class StreamSocialProducer:
    def __init__(self, config: Dict[str, Any], enable_chaos: bool = False):
        self.config = config
        self.metrics = MetricsCollector()
        self.chaos = ChaosMonkey() if enable_chaos else None
        self.producer: Optional[KafkaProducer] = None
        self.producer_id: Optional[str] = None
        self.sequence_numbers: Dict[int, int] = {}
        self.state = "UNINITIALIZED"
        self._setup_producer()
        
    def _setup_producer(self):
        """Initialize Kafka producer with idempotent configuration"""
        try:
            self.producer = KafkaProducer(
                **self.config,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            # Get producer ID (simulation - in real Kafka this is automatic)
            self.producer_id = f"producer-{uuid.uuid4().hex[:8]}"
            self.state = "READY"
            self.metrics.producer_state.set(1)
            
            logger.info(f"Producer initialized with ID: {self.producer_id}")
            
        except Exception as e:
            self.state = "ERROR"
            self.metrics.producer_state.set(2)
            logger.error(f"Failed to initialize producer: {e}")
            raise
            
    def send_post(self, user_id: str, content: str, post_id: str = None) -> Dict[str, Any]:
        """Send a post with idempotent guarantees"""
        if self.state != "READY":
            raise RuntimeError(f"Producer not ready, current state: {self.state}")
            
        if not post_id:
            post_id = str(uuid.uuid4())
            
        # Simulate chaos if enabled
        if self.chaos and self.chaos.should_fail():
            failure_duration = random.uniform(0.1, 2.0)
            logger.warning(f"Chaos monkey activated! Simulating {failure_duration:.2f}s failure")
            self.chaos.simulate_network_failure(failure_duration)
            self.metrics.retries.inc()
            
        post_data = {
            'post_id': post_id,
            'user_id': user_id,
            'content': content,
            'timestamp': time.time(),
            'producer_id': self.producer_id,
            'sequence_number': self._get_next_sequence(user_id)
        }
        
        start_time = time.time()
        
        try:
            # Send with partition key to ensure ordering per user
            future = self.producer.send(
                'streamsocial-posts',
                key=user_id,
                value=post_data
            )
            
            # Wait for acknowledgment
            record_metadata = future.get(timeout=10)
            
            self.metrics.messages_sent.inc()
            self.metrics.messages_acked.inc()
            self.metrics.send_latency.observe(time.time() - start_time)
            
            result = {
                'status': 'success',
                'post_id': post_id,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset,
                'producer_id': self.producer_id,
                'sequence_number': post_data['sequence_number']
            }
            
            logger.info(f"Post sent successfully: {post_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to send post {post_id}: {e}")
            self.metrics.retries.inc()
            raise
            
    def _get_next_sequence(self, partition_key: str) -> int:
        """Get next sequence number for partition"""
        partition_id = hash(partition_key) % 3  # Assume 3 partitions
        current_seq = self.sequence_numbers.get(partition_id, 0)
        self.sequence_numbers[partition_id] = current_seq + 1
        return current_seq + 1
        
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()
            self.state = "CLOSED"
            logger.info("Producer closed")
