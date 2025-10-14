import pytest
import json
import time
from unittest.mock import Mock, patch
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from validators.interceptors.producer_interceptor import SchemaValidatingProducer

class TestKafkaIntegration:
    @pytest.fixture
    def mock_producer(self):
        with patch('validators.interceptors.producer_interceptor.KafkaProducer') as mock:
            mock_instance = Mock()
            mock.return_value = mock_instance
            yield mock_instance
    
    @pytest.fixture
    def validating_producer(self, mock_producer):
        with patch('requests.post') as mock_post:
            # Mock successful validation
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"valid": True, "error": None}
            
            producer = SchemaValidatingProducer("localhost:9092")
            yield producer, mock_post
    
    def test_successful_validation_and_send(self, validating_producer):
        """Test successful event validation and sending"""
        producer, mock_post = validating_producer
        
        event = {
            "event_id": "test-id",
            "timestamp": "2025-01-15T10:30:00Z",
            "event_type": "profile_created",
            "user_id": "testuser",
            "profile_data": {"username": "test", "email": "test@example.com"},
            "metadata": {"source": "test", "version": "1.0.0"}
        }
        
        # Mock send method to return a future
        future_mock = Mock()
        future_mock.get.return_value = None
        producer.producer.send.return_value = future_mock
        
        result = producer.send_validated("user-events", "user.profile.v1", event)
        
        assert result is True
        mock_post.assert_called_once()
        producer.producer.send.assert_called_once_with("user-events", value=event, key=None)
    
    def test_validation_failure_sends_to_dlq(self, validating_producer):
        """Test that validation failures send events to DLQ"""
        producer, mock_post = validating_producer
        
        # Mock validation failure
        mock_post.return_value.json.return_value = {
            "valid": False, 
            "error": "Missing required field: user_id"
        }
        
        event = {"invalid": "event"}
        
        # Mock send method
        future_mock = Mock()
        future_mock.get.return_value = None
        producer.producer.send.return_value = future_mock
        
        result = producer.send_validated("user-events", "user.profile.v1", event)
        
        assert result is False
        # Should send to DLQ
        dlq_calls = [call for call in producer.producer.send.call_args_list 
                    if "dlq" in str(call)]
        assert len(dlq_calls) > 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
