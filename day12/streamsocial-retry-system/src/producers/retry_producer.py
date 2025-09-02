import json
import time
import uuid
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

from retry.failure_classifier import FailureClassifier, FailureType
from retry.backoff_calculator import ExponentialBackoffCalculator, BackoffState
from circuit_breaker.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from metrics.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)

class StreamSocialRetryProducer:
    """Intelligent Kafka producer with retry logic and circuit breaker"""
    
    def __init__(self, kafka_config: Dict[str, Any], retry_config: Dict[str, Any]):
        self.kafka_config = kafka_config
        self.retry_config = retry_config
        
        # Initialize components
        self.producer = KafkaProducer(**kafka_config)
        self.backoff_calculator = ExponentialBackoffCalculator(retry_config)
        self.failure_classifier = FailureClassifier()
        self.metrics_collector = MetricsCollector()
        
        # Circuit breakers per topic
        self.circuit_breakers = {}
        
        logger.info("StreamSocial Retry Producer initialized")
    
    def _get_circuit_breaker(self, topic: str) -> CircuitBreaker:
        """Get or create circuit breaker for topic"""
        if topic not in self.circuit_breakers:
            self.circuit_breakers[topic] = CircuitBreaker(
                name=f"producer-{topic}",
                failure_threshold=5,
                recovery_timeout=30
            )
        return self.circuit_breakers[topic]
    
    def send_with_retry(self, topic: str, value: Dict[str, Any], key: str = None) -> bool:
        """Send message with intelligent retry logic"""
        message_id = str(uuid.uuid4())
        value['message_id'] = message_id
        value['timestamp'] = time.time()
        
        circuit_breaker = self._get_circuit_breaker(topic)
        backoff_state = BackoffState()
        
        logger.info(f"Sending message {message_id} to topic {topic}")
        
        while backoff_state.attempt <= self.retry_config['max_retries']:
            try:
                # Check circuit breaker
                if circuit_breaker.state.value != 'closed':
                    circuit_breaker.call(self._send_message, topic, value, key)
                else:
                    self._send_message(topic, value, key)
                
                # Success!
                self.metrics_collector.record_retry_attempt(
                    success=True,
                    backoff_time=backoff_state.total_delay
                )
                logger.info(f"✅ Message {message_id} sent successfully on attempt {backoff_state.attempt + 1}")
                return True
                
            except CircuitBreakerOpenException as e:
                logger.warning(f"🔴 Circuit breaker open for topic {topic}: {e}")
                self.metrics_collector.record_circuit_breaker_trip()
                return False
                
            except Exception as e:
                failure_type = self.failure_classifier.classify(e)
                
                if failure_type == FailureType.PERMANENT:
                    logger.error(f"❌ Permanent failure for message {message_id}: {e}")
                    self.metrics_collector.record_retry_attempt(
                        success=False,
                        backoff_time=0,
                        failure_type='permanent'
                    )
                    return False
                
                # Handle transient failure
                if backoff_state.attempt >= self.retry_config['max_retries']:
                    logger.error(f"❌ Max retries exceeded for message {message_id}")
                    self.metrics_collector.record_retry_attempt(
                        success=False,
                        backoff_time=backoff_state.total_delay,
                        failure_type='max_retries'
                    )
                    return False
                
                # Calculate backoff and wait
                backoff_state = self.backoff_calculator.update_state(backoff_state)
                
                logger.warning(
                    f"⚠️ Transient failure for message {message_id} (attempt {backoff_state.attempt}): {e}. "
                    f"Retrying in {backoff_state.next_retry_time - time.time():.2f}s"
                )
                
                # Wait for backoff period
                while time.time() < backoff_state.next_retry_time:
                    time.sleep(0.1)
        
        return False
    
    def _send_message(self, topic: str, value: Dict[str, Any], key: str = None):
        """Internal method to send message to Kafka"""
        future = self.producer.send(topic, value=value, key=key)
        future.get(timeout=10)  # Wait for send to complete
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        metrics = self.metrics_collector.get_metrics()
        
        # Add circuit breaker states
        circuit_states = {}
        for topic, cb in self.circuit_breakers.items():
            circuit_states[topic] = {
                'state': cb.state.value,
                'failure_count': cb.metrics.failure_count,
                'success_count': cb.metrics.success_count
            }
        
        metrics['circuit_breakers'] = circuit_states
        return metrics
    
    def close(self):
        """Close producer and cleanup"""
        self.producer.close()
        logger.info("StreamSocial Retry Producer closed")
