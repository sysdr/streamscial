import unittest
import json
import time
import threading
from unittest.mock import Mock, patch
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from async_producer import ContentModerationProducer

class TestAsyncProducer(unittest.TestCase):
    
    def setUp(self):
        self.config = {
            'kafka': {'servers': ['localhost:9092']},
            'producer': {'acks': 1, 'retries': 3}
        }
        
        # Mock Kafka producer for tests
        with patch('async_producer.KafkaProducer'):
            self.producer = ContentModerationProducer(self.config)
    
    def test_send_for_moderation(self):
        """Test basic message sending"""
        result = self.producer.send_for_moderation('test_user', 'Hello world!')
        
        self.assertEqual(result['status'], 'queued')
        self.assertIn('message_id', result)
        self.assertEqual(self.producer.metrics['sent'], 1)
    
    def test_priority_calculation(self):
        """Test content priority calculation"""
        # Normal user, normal content
        self.assertEqual(
            self.producer._calculate_priority('user123', 'Nice day today!'),
            'normal'
        )
        
        # VIP user
        self.assertEqual(
            self.producer._calculate_priority('vip_alice', 'Hello!'),
            'high'
        )
        
        # Risky content
        self.assertEqual(
            self.producer._calculate_priority('user123', 'This is spam'),
            'high'
        )
    
    def test_success_callback(self):
        """Test successful delivery callback"""
        # Send message
        result = self.producer.send_for_moderation('test_user', 'Test message')
        message_id = result['message_id']
        
        # Simulate successful callback
        mock_metadata = Mock()
        mock_metadata.topic = 'content-moderation'
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        
        self.producer._on_send_success(mock_metadata, message_id)
        
        # Check metrics updated
        self.assertEqual(self.producer.metrics['delivered'], 1)
        self.assertNotIn(message_id, self.producer.in_flight_messages)
    
    def test_retriable_error_classification(self):
        """Test error classification"""
        from kafka.errors import RequestTimedOutError, NotLeaderForPartitionError, LeaderNotAvailableError
        
        # Retriable errors
        timeout_error = RequestTimedOutError()
        self.assertTrue(self.producer._is_retriable_error(timeout_error))
        
        leader_error = NotLeaderForPartitionError()
        self.assertTrue(self.producer._is_retriable_error(leader_error))
        
        # Non-retriable errors
        auth_error = Exception('AuthenticationError: Invalid credentials')
        self.assertFalse(self.producer._is_retriable_error(auth_error))
    
    def test_metrics_thread_safety(self):
        """Test metrics updates are thread-safe"""
        def send_messages():
            for i in range(100):
                self.producer.send_for_moderation(f'user_{i}', f'Message {i}')
        
        # Create multiple threads
        threads = [threading.Thread(target=send_messages) for _ in range(5)]
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Check total messages sent
        self.assertEqual(self.producer.metrics['sent'], 500)
    
    def test_circuit_breaker(self):
        """Test circuit breaker functionality"""
        cb = self.producer.circuit_breaker
        
        # Initially closed
        self.assertFalse(cb.is_open())
        
        # Record failures
        for _ in range(5):
            cb.record_failure()
        
        # Should be open now
        self.assertTrue(cb.is_open())
        
        # Record success should close it
        cb.record_success()
        self.assertFalse(cb.is_open())

if __name__ == '__main__':
    unittest.main()
