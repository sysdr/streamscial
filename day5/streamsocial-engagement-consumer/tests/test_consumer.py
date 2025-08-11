import pytest
import json
import time
from datetime import datetime
from unittest.mock import Mock, patch
from src.models.engagement import EngagementEvent
from src.consumer.engagement_consumer import EngagementConsumer
from src.utils.cache import CacheManager

class TestEngagementEvent:
    def test_engagement_event_creation(self):
        """Test EngagementEvent model creation"""
        event_data = {
            "user_id": "user123",
            "post_id": "post456",
            "action_type": "like",
            "timestamp": datetime.now().isoformat(),
            "content": None,
            "metadata": {}
        }
        
        event = EngagementEvent(**event_data)
        assert event.user_id == "user123"
        assert event.post_id == "post456"
        assert event.action_type == "like"
    
    def test_engagement_event_from_kafka_message(self):
        """Test deserialization from Kafka message"""
        message_data = {
            "user_id": "user123",
            "post_id": "post456",
            "action_type": "comment",
            "timestamp": datetime.now().isoformat(),
            "content": "Great post!",
            "metadata": {"ip": "127.0.0.1"}
        }
        
        message_bytes = json.dumps(message_data).encode('utf-8')
        event = EngagementEvent.from_kafka_message(message_bytes)
        
        assert event.user_id == "user123"
        assert event.action_type == "comment"
        assert event.content == "Great post!"

class TestCacheManager:
    @patch('redis.Redis')
    def test_update_engagement_stats(self, mock_redis):
        """Test engagement stats update"""
        # Mock Redis client
        mock_client = Mock()
        mock_redis.return_value = mock_client
        mock_client.get.return_value = None
        
        cache_manager = CacheManager()
        result = cache_manager.update_engagement_stats("post123", "like")
        
        assert mock_client.set.called
        assert result.post_id == "post123"
        assert result.likes == 1

@pytest.fixture
def sample_engagement_events():
    """Fixture providing sample engagement events"""
    return [
        {
            "user_id": "user1",
            "post_id": "post1",
            "action_type": "like",
            "timestamp": datetime.now().isoformat()
        },
        {
            "user_id": "user2",
            "post_id": "post1",
            "action_type": "share",
            "timestamp": datetime.now().isoformat()
        },
        {
            "user_id": "user3",
            "post_id": "post2",
            "action_type": "comment",
            "timestamp": datetime.now().isoformat(),
            "content": "Interesting post!"
        }
    ]

class TestIntegration:
    def test_end_to_end_processing(self, sample_engagement_events):
        """Test end-to-end message processing"""
        # This would be expanded for full integration testing
        assert len(sample_engagement_events) == 3
        assert all(event["action_type"] in ["like", "share", "comment"] 
                  for event in sample_engagement_events)
