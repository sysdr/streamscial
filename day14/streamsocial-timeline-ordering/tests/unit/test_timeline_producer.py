import pytest
from unittest.mock import Mock, patch
from src.producers.timeline_producer import TimelineProducer, TimelineMessage

class TestTimelineProducer:
    def test_timeline_message_creation(self):
        message = TimelineMessage("user123", "post456", "Hello world!")
        
        assert message.user_id == "user123"
        assert message.post_id == "post456"
        assert message.content == "Hello world!"
        assert message.timestamp is not None
        
        message_dict = message.to_dict()
        assert "created_at" in message_dict
        assert "timestamp" in message_dict
    
    @patch('src.producers.timeline_producer.KafkaProducer')
    def test_partition_key_calculation(self, mock_kafka_producer):
        # Mock the producer
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = TimelineProducer()
        
        key1 = producer.calculate_partition_key("user123")
        key2 = producer.calculate_partition_key("user123")
        key3 = producer.calculate_partition_key("user456")
        
        # Same user should get same key
        assert key1 == key2
        # Different users should get different keys
        assert key1 != key3
        assert key1.startswith("user:")
    
    @patch('src.producers.timeline_producer.KafkaProducer')
    def test_partition_calculation(self, mock_kafka_producer):
        # Mock the producer
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = TimelineProducer()
        
        # Same key should always map to same partition
        key = "user:test123"
        partition1 = producer.get_partition_for_key(key)
        partition2 = producer.get_partition_for_key(key)
        
        assert partition1 == partition2
        assert 0 <= partition1 < 6  # Should be within partition range
    
    @patch('src.producers.timeline_producer.KafkaProducer')
    def test_message_sending(self, mock_kafka_producer):
        # Mock the producer
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance
        
        # Mock the send method to return a future
        mock_future = Mock()
        mock_future.get.return_value = Mock(partition=2, offset=100)
        mock_producer_instance.send.return_value = mock_future
        
        producer = TimelineProducer()
        message = TimelineMessage("user123", "post456", "Test message")
        
        result = producer.send_timeline_message(message)
        
        assert result == True
        mock_producer_instance.send.assert_called_once()
        assert len(producer.sent_messages) == 1
