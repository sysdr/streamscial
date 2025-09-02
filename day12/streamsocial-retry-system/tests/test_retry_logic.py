import pytest
import time
from unittest.mock import Mock, patch
from kafka.errors import KafkaTimeoutError, AuthenticationFailedError

from src.retry.failure_classifier import FailureClassifier, FailureType
from src.retry.backoff_calculator import ExponentialBackoffCalculator, BackoffState
from src.circuit_breaker.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from src.producers.retry_producer import StreamSocialRetryProducer

class TestFailureClassifier:
    
    def test_transient_failures(self):
        """Test transient failure classification"""
        classifier = FailureClassifier()
        
        assert classifier.classify(KafkaTimeoutError()) == FailureType.TRANSIENT
        assert classifier.classify(ConnectionError()) == FailureType.TRANSIENT
        assert classifier.classify(OSError()) == FailureType.TRANSIENT
    
    def test_permanent_failures(self):
        """Test permanent failure classification"""
        classifier = FailureClassifier()
        
        assert classifier.classify(AuthenticationFailedError()) == FailureType.PERMANENT
        assert classifier.classify(ValueError()) == FailureType.PERMANENT
        assert classifier.classify(TypeError()) == FailureType.PERMANENT

class TestBackoffCalculator:
    
    def test_exponential_backoff(self):
        """Test exponential backoff calculation"""
        config = {
            'base_delay': 100,
            'max_delay': 30000,
            'backoff_multiplier': 2.0,
            'jitter_enabled': False,
            'jitter_factor': 0.0
        }
        
        calculator = ExponentialBackoffCalculator(config)
        
        # Test exponential progression
        assert calculator.calculate_delay(0) == 0.1  # 100ms
        assert calculator.calculate_delay(1) == 0.2  # 200ms
        assert calculator.calculate_delay(2) == 0.4  # 400ms
    
    def test_max_delay_cap(self):
        """Test maximum delay capping"""
        config = {
            'base_delay': 100,
            'max_delay': 1000,  # 1 second max
            'backoff_multiplier': 2.0,
            'jitter_enabled': False,
            'jitter_factor': 0.0
        }
        
        calculator = ExponentialBackoffCalculator(config)
        
        # Large attempt should be capped at max_delay
        assert calculator.calculate_delay(10) == 1.0  # Capped at 1 second
    
    def test_jitter_enabled(self):
        """Test jitter adds randomness"""
        config = {
            'base_delay': 1000,
            'max_delay': 30000,
            'backoff_multiplier': 2.0,
            'jitter_enabled': True,
            'jitter_factor': 0.25
        }
        
        calculator = ExponentialBackoffCalculator(config)
        
        # Multiple calculations should yield different results due to jitter
        delays = [calculator.calculate_delay(1) for _ in range(10)]
        assert len(set(delays)) > 1  # Should have variation

class TestCircuitBreaker:
    
    def test_circuit_breaker_opens_on_failures(self):
        """Test circuit breaker opens after failure threshold"""
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=1)
        
        def failing_function():
            raise Exception("Test failure")
        
        # Execute function multiple times to trigger failures
        for i in range(3):
            try:
                cb.call(failing_function)
            except Exception:
                pass
        
        # Circuit should now be open
        with pytest.raises(CircuitBreakerOpenException):
            cb.call(failing_function)
    
    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after timeout"""
        cb = CircuitBreaker("test", failure_threshold=2, recovery_timeout=0.1)  # 100ms recovery
        
        def failing_function():
            raise Exception("Test failure")
        
        # Trigger circuit opening
        for i in range(2):
            try:
                cb.call(failing_function)
            except Exception:
                pass
        
        # Wait for recovery timeout
        time.sleep(0.2)
        
        # Circuit should now be in half-open state
        def working_function():
            return "success"
        
        # This should succeed and close the circuit
        result = cb.call(working_function)
        assert result == "success"

class TestRetryProducer:
    
    @patch('src.producers.retry_producer.KafkaProducer')
    def test_successful_send_no_retry(self, mock_kafka_producer):
        """Test successful send without retries"""
        # Setup mock
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_future.get.return_value = True
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance
        
        # Test
        config = {
            'bootstrap_servers': ['localhost:9092'],
            'value_serializer': lambda x: str(x).encode('utf-8')
        }
        retry_config = {
            'max_retries': 3,
            'base_delay': 100,
            'max_delay': 30000,
            'backoff_multiplier': 2.0,
            'jitter_enabled': False,
            'jitter_factor': 0.0
        }
        
        producer = StreamSocialRetryProducer(config, retry_config)
        
        message = {'content': 'test message'}
        result = producer.send_with_retry('test-topic', message)
        
        assert result == True
        mock_producer_instance.send.assert_called_once()
    
    @patch('src.producers.retry_producer.KafkaProducer')
    def test_retry_on_transient_failure(self, mock_kafka_producer):
        """Test retry behavior on transient failures"""
        # Setup mock to fail once then succeed
        mock_producer_instance = Mock()
        mock_future = Mock()
        
        # First call fails, second succeeds
        mock_future.get.side_effect = [KafkaTimeoutError("Timeout"), True]
        mock_producer_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_producer_instance
        
        config = {
            'bootstrap_servers': ['localhost:9092'],
            'value_serializer': lambda x: str(x).encode('utf-8')
        }
        retry_config = {
            'max_retries': 3,
            'base_delay': 1,  # Very short delay for testing
            'max_delay': 1000,
            'backoff_multiplier': 2.0,
            'jitter_enabled': False,
            'jitter_factor': 0.0
        }
        
        producer = StreamSocialRetryProducer(config, retry_config)
        
        message = {'content': 'test message'}
        result = producer.send_with_retry('test-topic', message)
        
        assert result == True
        assert mock_producer_instance.send.call_count == 2  # One failure, one success

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
