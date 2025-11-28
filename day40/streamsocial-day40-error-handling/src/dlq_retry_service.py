"""DLQ Retry Service - Background worker for processing failed records"""
import json
import time
import random
from typing import Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError
from dataclasses import dataclass
import redis

@dataclass
class RetryStats:
    total_retries: int = 0
    successful_retries: int = 0
    failed_retries: int = 0
    permanent_failures: int = 0

class DLQRetryService:
    """Service to consume DLQ and retry failed moderation"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consumer = Consumer({
            'bootstrap.servers': config.get('bootstrap.servers', 'localhost:9092'),
            'group.id': 'dlq-retry-service',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        
        self.producer = Producer({
            'bootstrap.servers': config.get('bootstrap.servers', 'localhost:9092')
        })
        
        self.dlq_topic = config.get('dlq.topic', 'moderation-dlq')
        self.max_retries = int(config.get('dlq.max.retries', 10))
        self.initial_delay = float(config.get('dlq.initial.delay', 1.0))
        self.max_delay = float(config.get('dlq.max.delay', 60.0))
        
        # Redis for retry tracking
        self.redis_client = redis.Redis(
            host=config.get('redis.host', 'localhost'),
            port=int(config.get('redis.port', 6379)),
            decode_responses=True
        )
        
        self.stats = RetryStats()
        
        print(f"✓ DLQ Retry Service initialized")
        print(f"  DLQ Topic: {self.dlq_topic}")
        print(f"  Max Retries: {self.max_retries}")
    
    def get_retry_count(self, record_key: str) -> int:
        """Get current retry count from Redis"""
        key = f"retry:{record_key}"
        count = self.redis_client.hget(key, 'attempts')
        return int(count) if count else 0
    
    def increment_retry_count(self, record_key: str, error_type: str):
        """Increment retry count in Redis"""
        key = f"retry:{record_key}"
        self.redis_client.hincrby(key, 'attempts', 1)
        self.redis_client.hset(key, 'last_attempt', int(time.time() * 1000))
        self.redis_client.hset(key, 'error_type', error_type)
        self.redis_client.expire(key, 86400)  # TTL 24 hours
    
    def attempt_moderation(self, content: str) -> bool:
        """Retry moderation with improved success rate"""
        # Simulate improved success rate for retries (80% success)
        rand = random.random()
        
        if rand < 0.20:  # 20% still fail
            raise Exception('ServiceUnavailable: Still experiencing issues')
        
        time.sleep(0.005)  # Simulate processing
        return True
    
    def calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter"""
        delay = min(self.initial_delay * (2 ** attempt), self.max_delay)
        jitter = random.uniform(0, delay * 0.1)
        return delay + jitter
    
    def process_dlq_record(self, msg):
        """Process a single DLQ record with retry logic"""
        # Extract metadata from headers
        headers = {h[0]: h[1].decode('utf-8') for h in msg.headers()}
        
        record_key = msg.key().decode('utf-8') if msg.key() else 'null'
        value = json.loads(msg.value().decode('utf-8'))
        content = value.get('content', '')
        
        error_class = headers.get('__connect.errors.class.name', 'Unknown')
        error_msg = headers.get('__connect.errors.exception.message', '')
        original_topic = headers.get('__connect.errors.topic', 'unknown')
        
        print(f"\n→ Processing DLQ record: {record_key}")
        print(f"  Original Topic: {original_topic}")
        print(f"  Error Type: {error_class}")
        print(f"  Error Message: {error_msg[:50]}...")
        
        # Get current retry count
        retry_count = self.get_retry_count(record_key)
        print(f"  Retry Attempt: {retry_count + 1}/{self.max_retries}")
        
        if retry_count >= self.max_retries:
            print(f"✗ Max retries exhausted for {record_key} - Marking as permanent failure")
            self.stats.permanent_failures += 1
            self.store_permanent_failure(record_key, value, error_class)
            return True  # Remove from DLQ
        
        # Apply exponential backoff
        delay = self.calculate_backoff(retry_count)
        print(f"  Waiting {delay:.1f}s before retry...")
        time.sleep(delay)
        
        # Attempt retry
        try:
            self.stats.total_retries += 1
            success = self.attempt_moderation(content)
            
            if success:
                print(f"✓ Retry successful for {record_key}")
                self.stats.successful_retries += 1
                # Clear Redis tracking
                self.redis_client.delete(f"retry:{record_key}")
                return True  # Remove from DLQ
            else:
                raise Exception("Moderation failed")
                
        except Exception as e:
            print(f"✗ Retry failed for {record_key}: {e}")
            self.stats.failed_retries += 1
            self.increment_retry_count(record_key, error_class)
            
            # Requeue to DLQ for another retry
            self.producer.produce(
                topic=self.dlq_topic,
                key=msg.key(),
                value=msg.value(),
                headers=msg.headers()
            )
            self.producer.flush()
            return True  # Remove original from DLQ (requeued with updated headers)
    
    def store_permanent_failure(self, record_key: str, value: Dict, error_type: str):
        """Store permanently failed records in Redis for manual review"""
        key = f"permanent_failure:{record_key}"
        self.redis_client.hset(key, mapping={
            'content': json.dumps(value),
            'error_type': error_type,
            'failed_at': int(time.time() * 1000),
            'retry_attempts': self.max_retries
        })
        print(f"  → Stored in permanent failure store for manual review")
    
    def run(self):
        """Main retry service loop"""
        self.consumer.subscribe([self.dlq_topic])
        print(f"\n✓ Subscribed to DLQ: {self.dlq_topic}")
        print("\nProcessing DLQ records (Ctrl+C to stop)...\n")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                # Process DLQ record
                should_commit = self.process_dlq_record(msg)
                
                if should_commit:
                    self.consumer.commit(message=msg)
                
                time.sleep(0.1)  # Rate limiting
                
        except KeyboardInterrupt:
            print("\n\nShutdown signal received...")
        finally:
            self.print_stats()
            self.consumer.close()
    
    def print_stats(self):
        """Print retry statistics"""
        print("\n" + "="*50)
        print("DLQ RETRY SERVICE STATISTICS")
        print("="*50)
        print(f"Total Retries:        {self.stats.total_retries}")
        print(f"Successful Retries:   {self.stats.successful_retries}")
        print(f"Failed Retries:       {self.stats.failed_retries}")
        print(f"Permanent Failures:   {self.stats.permanent_failures}")
        if self.stats.total_retries > 0:
            success_rate = (self.stats.successful_retries / self.stats.total_retries) * 100
            print(f"Success Rate:         {success_rate:.1f}%")
        print("="*50)

if __name__ == '__main__':
    config = {
        'bootstrap.servers': 'localhost:9092',
        'dlq.topic': 'moderation-dlq',
        'dlq.max.retries': '10',
        'dlq.initial.delay': '1.0',
        'dlq.max.delay': '60.0',
        'redis.host': 'localhost',
        'redis.port': '6379'
    }
    
    service = DLQRetryService(config)
    service.run()
