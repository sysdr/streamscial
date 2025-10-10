import pytest
import json
from src.error_handling.error_classifier import ErrorClassifier, ErrorType

class TestErrorClassifier:
    def setup_method(self):
        self.classifier = ErrorClassifier()
        
    def test_json_decode_error_classification(self):
        """Test JSON decode error classification"""
        error = json.JSONDecodeError("Invalid JSON", '{"invalid": json', 0)
        context = {'attempt_count': 0}
        
        result = self.classifier.classify_error(error, context)
        assert result == ErrorType.DLQ
        
    def test_recoverable_json_error(self):
        """Test recoverable JSON errors are skipped"""
        error = json.JSONDecodeError("Missing emoji field", '{"content": "test"}', 0)
        context = {'attempt_count': 0}
        
        # Mock the method to return True for this test
        self.classifier._is_recoverable_json_error = lambda msg: 'emoji' in msg
        
        result = self.classifier.classify_error(error, context)
        assert result == ErrorType.SKIP
        
    def test_key_error_retry_logic(self):
        """Test KeyError retry logic"""
        error = KeyError("missing_field")
        context = {'attempt_count': 2}
        
        result = self.classifier.classify_error(error, context)
        assert result == ErrorType.RETRY
        
    def test_key_error_to_dlq_after_retries(self):
        """Test KeyError goes to DLQ after max retries"""
        error = KeyError("missing_field")
        context = {'attempt_count': 5}
        
        result = self.classifier.classify_error(error, context)
        assert result == ErrorType.DLQ
        
    def test_connection_error_always_retry(self):
        """Test ConnectionError always retries"""
        error = ConnectionError("Network unreachable")
        context = {'attempt_count': 10}
        
        result = self.classifier.classify_error(error, context)
        assert result == ErrorType.RETRY
        
    def test_memory_error_fatal(self):
        """Test MemoryError is classified as fatal"""
        error = MemoryError("Out of memory")
        context = {'attempt_count': 0}
        
        result = self.classifier.classify_error(error, context)
        assert result == ErrorType.FATAL
        
    def test_unknown_error_to_dlq(self):
        """Test unknown errors go to DLQ"""
        error = RuntimeError("Unknown error")
        context = {'attempt_count': 0}
        
        result = self.classifier.classify_error(error, context)
        assert result == ErrorType.DLQ
