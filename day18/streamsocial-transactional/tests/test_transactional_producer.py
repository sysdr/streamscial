"""Tests for StreamSocial transactional producer."""

import pytest
import time
import uuid
from datetime import datetime
from unittest.mock import Mock, patch

from src.streamsocial.models.post import Post, ContentType, PostStatus
from src.kafka_client.producers.transactional_producer import StreamSocialTransactionalProducer, TransactionError

class TestStreamSocialTransactionalProducer:
    
    @pytest.fixture
    def producer(self):
        """Create a test producer instance."""
        with patch('src.kafka_client.producers.transactional_producer.KafkaProducer') as mock_producer:
            mock_producer_instance = Mock()
            # Mock the metrics method to return a proper dictionary-like object
            mock_producer_instance.metrics.return_value = {
                'messages-sent': 0,
                'bytes-sent': 0,
                'send-rate': 0.0,
                'batch-size-avg': 0.0
            }
            mock_producer.return_value = mock_producer_instance
            
            producer = StreamSocialTransactionalProducer("test-producer")
            yield producer
            producer.close()
    
    @pytest.fixture
    def sample_post(self):
        """Create a sample post for testing."""
        return Post(
            post_id="test_post_123",
            user_id="user_alice",
            username="alice",
            content="Testing transactional posting!",
            content_type=ContentType.TEXT,
            status=PostStatus.PUBLISHED,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            hashtags=["test", "atomic", "kafka"]
        )
    
    def test_producer_initialization(self, producer):
        """Test producer initializes correctly."""
        assert producer.transaction_id.startswith("test-producer")
        assert not producer.transaction_active
        assert len(producer.message_buffer) == 0
    
    def test_transaction_context_manager(self, producer):
        """Test transaction context manager."""
        assert not producer.transaction_active
        
        with producer.transaction():
            assert producer.transaction_active
        
        assert not producer.transaction_active
    
    def test_nested_transaction_raises_error(self, producer):
        """Test that nested transactions raise an error."""
        with producer.transaction():
            with pytest.raises(TransactionError):
                with producer.transaction():
                    pass
    
    def test_send_message_outside_transaction_raises_error(self, producer):
        """Test sending message outside transaction context raises error."""
        with pytest.raises(TransactionError):
            producer._send_message("test-topic", "test-key", {"data": "test"})
    
    def test_atomic_post_creation_success(self, producer, sample_post):
        """Test successful atomic post creation."""
        followers = ["user_bob", "user_carol", "user_david"]
        
        with patch.object(producer.producer, 'send') as mock_send:
            mock_send.return_value = Mock()
            
            result = producer.atomic_post_creation(sample_post, followers)
            
            assert result is True
            # Verify all expected topics were called
            assert mock_send.call_count >= 4  # timeline, feed, notifications, analytics
    
    def test_atomic_post_creation_failure(self, producer, sample_post):
        """Test atomic post creation handles failures."""
        followers = ["user_bob"]
        
        with patch.object(producer.producer, 'send') as mock_send:
            mock_send.side_effect = Exception("Kafka connection failed")
            
            result = producer.atomic_post_creation(sample_post, followers)
            
            assert result is False
    
    def test_multi_user_timeline_update(self, producer, sample_post):
        """Test atomic multi-user timeline update."""
        target_users = ["user_bob", "user_carol"]
        
        with patch.object(producer.producer, 'send') as mock_send:
            mock_send.return_value = Mock()
            
            result = producer.atomic_multi_user_timeline_update(sample_post, target_users)
            
            assert result is True
            # Should send to timeline for each user plus analytics
            assert mock_send.call_count == len(target_users) + 1
    
    def test_transaction_metrics(self, producer):
        """Test transaction metrics collection."""
        metrics = producer.get_transaction_metrics()
        
        assert 'transaction_id' in metrics
        assert 'transaction_active' in metrics
        assert 'buffered_messages' in metrics
        assert 'topics' in metrics
        
        assert metrics['transaction_active'] is False
        assert len(metrics['topics']) > 0

@pytest.mark.integration
class TestTransactionalProducerIntegration:
    """Integration tests requiring running Kafka."""
    
    @pytest.fixture(scope="class")
    def kafka_producer(self):
        """Create a real producer for integration testing."""
        # This would require actual Kafka running
        pytest.skip("Integration test requires running Kafka cluster")
    
    def test_real_transaction_flow(self, kafka_producer):
        """Test actual transaction flow with real Kafka."""
        # Integration test implementation
        pass

if __name__ == "__main__":
    pytest.main([__file__])
