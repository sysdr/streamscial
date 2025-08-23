import pytest
import asyncio
import json
import uuid
from datetime import datetime
from unittest.mock import Mock, AsyncMock

from src.models.engagement import EngagementEvent
from src.consumers.engagement_consumer import ReliableEngagementConsumer
from src.utils.database import DatabaseManager
from src.monitoring.metrics import MetricsCollector

@pytest.fixture
def sample_engagement():
    return EngagementEvent(
        user_id="test_user_123",
        content_id="test_content_456",
        engagement_type="like",
        timestamp=datetime.utcnow(),
        metadata={"device": "mobile", "location": "US"},
        event_id=str(uuid.uuid4())
    )

@pytest.fixture
def mock_db_manager():
    db_manager = Mock(spec=DatabaseManager)
    db_manager.is_duplicate.return_value = False
    db_manager.store_engagement.return_value = True
    db_manager.get_session.return_value = Mock()
    return db_manager

@pytest.fixture
def mock_metrics():
    return Mock(spec=MetricsCollector)

@pytest.fixture
def consumer_config():
    return {
        'bootstrap_servers': 'localhost:9092',
        'group_id': 'test-group',
        'topic': 'test-engagements',
        'batch_size': 10
    }

class TestReliableEngagementConsumer:
    def test_validate_engagement_success(self, mock_db_manager, mock_metrics, consumer_config, sample_engagement):
        consumer = ReliableEngagementConsumer(consumer_config, mock_db_manager, mock_metrics)
        
        result = consumer.validate_engagement(sample_engagement)
        
        assert result is True
        mock_db_manager.is_duplicate.assert_called_once_with(sample_engagement.event_id)
    
    def test_validate_engagement_missing_fields(self, mock_db_manager, mock_metrics, consumer_config):
        consumer = ReliableEngagementConsumer(consumer_config, mock_db_manager, mock_metrics)
        
        # Create engagement with missing user_id
        invalid_engagement = EngagementEvent(
            user_id="",  # Empty user_id
            content_id="test_content",
            engagement_type="like",
            timestamp=datetime.utcnow(),
            metadata={},
            event_id="test_id"
        )
        
        result = consumer.validate_engagement(invalid_engagement)
        
        assert result is False
    
    def test_validate_engagement_invalid_type(self, mock_db_manager, mock_metrics, consumer_config, sample_engagement):
        consumer = ReliableEngagementConsumer(consumer_config, mock_db_manager, mock_metrics)
        
        sample_engagement.engagement_type = "invalid_type"
        
        result = consumer.validate_engagement(sample_engagement)
        
        assert result is False
    
    def test_validate_engagement_duplicate(self, mock_db_manager, mock_metrics, consumer_config, sample_engagement):
        consumer = ReliableEngagementConsumer(consumer_config, mock_db_manager, mock_metrics)
        mock_db_manager.is_duplicate.return_value = True
        
        result = consumer.validate_engagement(sample_engagement)
        
        assert result is False
    
    def test_store_engagement_batch_success(self, mock_db_manager, mock_metrics, consumer_config, sample_engagement):
        consumer = ReliableEngagementConsumer(consumer_config, mock_db_manager, mock_metrics)
        
        events = [sample_engagement]
        result = consumer.store_engagement_batch(events)
        
        assert result is True
    
    def test_store_engagement_batch_failure(self, mock_db_manager, mock_metrics, consumer_config, sample_engagement):
        consumer = ReliableEngagementConsumer(consumer_config, mock_db_manager, mock_metrics)
        mock_db_manager.store_engagement.side_effect = Exception("Database error")
        
        events = [sample_engagement]
        result = consumer.store_engagement_batch(events)
        
        assert result is False

if __name__ == "__main__":
    pytest.main([__file__])
