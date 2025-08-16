import pytest
import time
import threading
from unittest.mock import Mock, patch
from src.producer.high_volume_producer import (
    HighVolumeProducer, 
    ProducerConfig, 
    UserAction,
    ConnectionPool,
    ProducerMetrics
)

class TestProducerMetrics:
    def test_initial_metrics(self):
        metrics = ProducerMetrics()
        stats = metrics.get_stats()
        
        assert stats['messages_sent'] == 0
        assert stats['messages_failed'] == 0
        assert stats['success_rate'] == 0

    def test_record_success(self):
        metrics = ProducerMetrics()
        metrics.record_success(1024, 0.1)
        
        stats = metrics.get_stats()
        assert stats['messages_sent'] == 1
        assert stats['bytes_sent'] == 1024
        assert stats['avg_latency_ms'] == 100  # 0.1s = 100ms

    def test_record_failure(self):
        metrics = ProducerMetrics()
        metrics.record_failure()
        
        stats = metrics.get_stats()
        assert stats['messages_failed'] == 1

class TestUserAction:
    def test_user_action_creation(self):
        action = UserAction(
            user_id="user123",
            action_type="post",
            content_id="content456",
            timestamp=1234567890,
            metadata={"device": "mobile"}
        )
        
        assert action.user_id == "user123"
        assert action.action_type == "post"
        assert action.metadata["device"] == "mobile"

@pytest.fixture
def mock_config():
    return ProducerConfig(
        bootstrap_servers="localhost:9092",
        batch_size=1024,
        linger_ms=5
    )

class TestHighVolumeProducer:
    @patch('src.producer.high_volume_producer.Producer')
    def test_producer_initialization(self, mock_producer_class, mock_config):
        producer = HighVolumeProducer(mock_config, pool_size=2, worker_threads=2)
        
        assert producer.config == mock_config
        assert producer.worker_threads == 2
        assert not producer.running

    @patch('src.producer.high_volume_producer.Producer')
    def test_producer_start_stop(self, mock_producer_class, mock_config):
        producer = HighVolumeProducer(mock_config, pool_size=2, worker_threads=2)
        
        # Start producer
        producer.start()
        assert producer.running
        assert len(producer.workers) == 2
        
        # Stop producer
        producer.stop()
        assert not producer.running

    @patch('src.producer.high_volume_producer.Producer')
    def test_send_async(self, mock_producer_class, mock_config):
        producer = HighVolumeProducer(mock_config, pool_size=1, worker_threads=1)
        producer.start()
        
        action = UserAction(
            user_id="user123",
            action_type="post", 
            content_id="content456",
            timestamp=int(time.time() * 1000),
            metadata={}
        )
        
        result = producer.send_async("test-topic", action)
        assert result == True
        
        producer.stop()

if __name__ == '__main__':
    pytest.main([__file__])
