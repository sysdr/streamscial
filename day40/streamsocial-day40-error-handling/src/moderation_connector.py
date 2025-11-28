"""Custom Kafka Connect Sink Connector for Content Moderation with Error Handling"""
import json
import time
import random
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer, KafkaError
from dataclasses import dataclass
from enum import Enum

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class ErrorMetrics:
    total_errors: int = 0
    retryable_errors: int = 0
    non_retryable_errors: int = 0
    dlq_records: int = 0
    circuit_breaker_trips: int = 0

class CircuitBreaker:
    """Circuit breaker for ML API calls"""
    def __init__(self, failure_threshold: int = 50, timeout: int = 120, success_threshold: int = 10):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.success_threshold = success_threshold
        self.failure_count = 0
        self.success_count = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time = None
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == CircuitState.OPEN:
            elapsed = time.time() - self.last_failure_time if self.last_failure_time else 0
            if elapsed > self.timeout:
                print(f"Circuit breaker transitioning to HALF_OPEN after {elapsed:.1f}s")
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
            else:
                raise Exception(f"Circuit breaker OPEN - service unavailable (retry in {self.timeout - elapsed:.1f}s)")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        self.failure_count = 0
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                print(f"Circuit breaker CLOSED after {self.success_count} successful calls")
                self.state = CircuitState.CLOSED
    
    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            print(f"Circuit breaker OPENED after {self.failure_count} failures")
            self.state = CircuitState.OPEN
    
    def get_state(self) -> str:
        return self.state.value

class ModerationSinkConnector:
    """Content moderation sink with comprehensive error handling"""
    
    RETRYABLE_ERRORS = {
        'ServiceUnavailable',
        'TimeoutError',
        'RateLimitExceeded',
        'ConnectionError'
    }
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consumer = Consumer({
            'bootstrap.servers': config.get('bootstrap.servers', 'localhost:9092'),
            'group.id': config.get('group.id', 'moderation-sink-group'),
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        
        self.dlq_producer = Producer({
            'bootstrap.servers': config.get('bootstrap.servers', 'localhost:9092'),
            'acks': 'all'
        })
        
        self.topics = config.get('topics', 'user-posts').split(',')
        self.dlq_topic = config.get('errors.deadletterqueue.topic.name', 'moderation-dlq')
        self.error_tolerance = config.get('errors.tolerance', 'all')
        self.retry_timeout = int(config.get('errors.retry.timeout', 300000))  # 5 minutes
        self.retry_max_delay = int(config.get('errors.retry.delay.max.ms', 60000))  # 1 minute
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=int(config.get('moderation.circuit.breaker.failure.threshold', 50)),
            timeout=int(config.get('moderation.circuit.breaker.reset.timeout', 120000)) / 1000
        )
        
        self.metrics = ErrorMetrics()
        self.retry_counts = {}
        
        print(f"✓ Moderation connector initialized with DLQ: {self.dlq_topic}")
        print(f"  Error tolerance: {self.error_tolerance}")
        print(f"  Retry timeout: {self.retry_timeout}ms")
    
    def moderate_content(self, content: str) -> Dict[str, Any]:
        """Simulate ML-based content moderation with various failure scenarios"""
        # Simulate different failure types
        rand = random.random()
        
        if rand < 0.05:  # 5% encoding errors
            raise UnicodeDecodeError('utf-8', b'', 0, 1, 'Invalid UTF-8 sequence in content')
        elif rand < 0.10:  # 5% service unavailable
            raise Exception('ServiceUnavailable: ML service temporarily down')
        elif rand < 0.13:  # 3% timeout
            raise Exception('TimeoutError: ML API request timeout')
        elif rand < 0.15:  # 2% rate limit
            raise Exception('RateLimitExceeded: Too many requests')
        
        # Successful moderation
        time.sleep(0.002)  # Simulate 2ms processing
        
        # Simple toxicity detection (keyword-based for demo)
        toxic_keywords = ['spam', 'scam', 'hate', 'violence']
        is_toxic = any(keyword in content.lower() for keyword in toxic_keywords)
        
        return {
            'content': content,
            'is_toxic': is_toxic,
            'confidence': random.uniform(0.7, 0.99),
            'timestamp': int(time.time() * 1000),
            'moderation_latency_ms': 2
        }
    
    def classify_error(self, error: Exception) -> tuple[bool, str]:
        """Classify error as retryable or not"""
        error_msg = str(error)
        error_class = type(error).__name__
        
        # Check if error message contains retryable keywords
        for retryable in self.RETRYABLE_ERRORS:
            if retryable in error_msg or retryable in error_class:
                return True, retryable
        
        return False, error_class
    
    def send_to_dlq(self, record, error: Exception, attempt: int):
        """Send failed record to Dead Letter Queue with metadata"""
        try:
            # Build DLQ record with comprehensive headers
            headers = {
                '__connect.errors.topic': record.topic(),
                '__connect.errors.partition': str(record.partition()),
                '__connect.errors.offset': str(record.offset()),
                '__connect.errors.connector.name': 'content-moderation-sink',
                '__connect.errors.task.id': '1',
                '__connect.errors.stage': 'PROCESSING',
                '__connect.errors.class.name': type(error).__name__,
                '__connect.errors.exception.message': str(error),
                '__connect.errors.timestamp': str(int(time.time() * 1000)),
                '__connect.errors.attempt': str(attempt)
            }
            
            # Convert headers to list of tuples
            header_list = [(k, v.encode('utf-8')) for k, v in headers.items()]
            
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                key=record.key(),
                value=record.value(),
                headers=header_list
            )
            self.dlq_producer.flush()
            
            self.metrics.dlq_records += 1
            print(f"  → Sent to DLQ: {record.key().decode('utf-8') if record.key() else 'null'} "
                  f"(Error: {type(error).__name__})")
        except Exception as dlq_error:
            print(f"✗ Failed to send to DLQ: {dlq_error}")
    
    def process_with_retry(self, record) -> bool:
        """Process record with retry logic"""
        key = record.key().decode('utf-8') if record.key() else 'null'
        value = json.loads(record.value().decode('utf-8'))
        content = value.get('content', '')
        
        max_attempts = 3
        initial_delay = 1  # seconds
        
        for attempt in range(max_attempts):
            try:
                # Use circuit breaker for ML API call
                result = self.circuit_breaker.call(self.moderate_content, content)
                
                # Success!
                print(f"✓ Moderated: {key} - Toxic: {result['is_toxic']} "
                      f"(confidence: {result['confidence']:.2f})")
                return True
                
            except Exception as e:
                is_retryable, error_type = self.classify_error(e)
                self.metrics.total_errors += 1
                
                if is_retryable:
                    self.metrics.retryable_errors += 1
                    if attempt < max_attempts - 1:
                        # Calculate exponential backoff with jitter
                        delay = min(initial_delay * (2 ** attempt), self.retry_max_delay / 1000)
                        jitter = random.uniform(0, delay * 0.1)
                        time.sleep(delay + jitter)
                        print(f"  ↻ Retry {attempt + 1}/{max_attempts} for {key} after {delay:.1f}s")
                        continue
                else:
                    self.metrics.non_retryable_errors += 1
                    print(f"✗ Non-retryable error for {key}: {error_type}")
                
                # Failed after retries or non-retryable
                if self.error_tolerance == 'all':
                    self.send_to_dlq(record, e, attempt + 1)
                    return False  # Continue processing
                else:
                    raise e  # Fail fast
        
        # Exhausted retries
        self.send_to_dlq(record, Exception("Max retries exhausted"), max_attempts)
        return False
    
    def run(self):
        """Main connector processing loop"""
        self.consumer.subscribe(self.topics)
        print(f"\n✓ Subscribed to topics: {self.topics}")
        print(f"✓ Circuit breaker: {self.circuit_breaker.get_state()}")
        print("\nProcessing records (Ctrl+C to stop)...\n")
        
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
                
                # Process with error handling
                self.process_with_retry(msg)
                
                # Commit offset
                self.consumer.commit(message=msg)
                
        except KeyboardInterrupt:
            print("\n\nShutdown signal received...")
        finally:
            self.print_metrics()
            self.consumer.close()
            self.dlq_producer.flush()
    
    def print_metrics(self):
        """Print error handling metrics"""
        print("\n" + "="*50)
        print("ERROR HANDLING METRICS")
        print("="*50)
        print(f"Total Errors:          {self.metrics.total_errors}")
        print(f"Retryable Errors:      {self.metrics.retryable_errors}")
        print(f"Non-Retryable Errors:  {self.metrics.non_retryable_errors}")
        print(f"DLQ Records:           {self.metrics.dlq_records}")
        print(f"Circuit Breaker State: {self.circuit_breaker.get_state()}")
        print(f"Circuit Trips:         {self.metrics.circuit_breaker_trips}")
        print("="*50)

if __name__ == '__main__':
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'moderation-sink-group',
        'topics': 'user-posts',
        'errors.tolerance': 'all',
        'errors.deadletterqueue.topic.name': 'moderation-dlq',
        'errors.retry.timeout': '300000',
        'errors.retry.delay.max.ms': '60000',
        'moderation.circuit.breaker.failure.threshold': '50',
        'moderation.circuit.breaker.reset.timeout': '120000'
    }
    
    connector = ModerationSinkConnector(config)
    connector.run()
