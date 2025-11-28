"""Tests for error handling components"""
import pytest
import json
from src.moderation_connector import CircuitBreaker, ModerationSinkConnector

def test_circuit_breaker_closed_state():
    """Test circuit breaker in normal closed state"""
    cb = CircuitBreaker(failure_threshold=3, timeout=5)
    
    def success_func():
        return "success"
    
    result = cb.call(success_func)
    assert result == "success"
    assert cb.state.value == "closed"

def test_circuit_breaker_opens_on_failures():
    """Test circuit breaker opens after threshold failures"""
    cb = CircuitBreaker(failure_threshold=3, timeout=5)
    
    def failing_func():
        raise Exception("Test failure")
    
    # Trigger failures
    for _ in range(3):
        try:
            cb.call(failing_func)
        except:
            pass
    
    assert cb.state.value == "open"
    
    # Verify it rejects calls when open
    with pytest.raises(Exception, match="Circuit breaker"):
        cb.call(failing_func)

def test_error_classification():
    """Test error classification logic"""
    config = {
        'bootstrap.servers': 'localhost:9092',
        'topics': 'test-topic'
    }
    connector = ModerationSinkConnector(config)
    
    # Retryable error
    error1 = Exception("ServiceUnavailable: Service down")
    is_retryable, error_type = connector.classify_error(error1)
    assert is_retryable == True
    
    # Non-retryable error
    error2 = UnicodeDecodeError('utf-8', b'', 0, 1, 'Invalid UTF-8')
    is_retryable, error_type = connector.classify_error(error2)
    assert is_retryable == False

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
