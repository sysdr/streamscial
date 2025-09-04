import pytest
import time
import uuid
from unittest.mock import Mock, patch
from prometheus_client import REGISTRY, CollectorRegistry
from src.idempotent_producer import StreamSocialProducer
from config.kafka_config import KAFKA_CONFIG

class TestIdempotentProducer:
    
    def setup_method(self):
        """Clear Prometheus registry before each test"""
        # Clear the default registry
        for collector in list(REGISTRY._collector_to_names.keys()):
            REGISTRY.unregister(collector)
    
    def test_producer_initialization(self):
        """Test producer initializes correctly"""
        with patch('src.idempotent_producer.KafkaProducer'):
            producer = StreamSocialProducer(KAFKA_CONFIG)
            assert producer.state == "READY"
            assert producer.producer_id is not None
            
    def test_sequence_number_increment(self):
        """Test sequence numbers increment correctly"""
        with patch('src.idempotent_producer.KafkaProducer'):
            producer = StreamSocialProducer(KAFKA_CONFIG)
            
            seq1 = producer._get_next_sequence("user1")
            seq2 = producer._get_next_sequence("user1")
            seq3 = producer._get_next_sequence("user2")
            
            assert seq2 == seq1 + 1
            # Different partition may have different sequence number based on hash
            # Just verify it's a positive integer
            assert seq3 > 0
            
    def test_post_sending_success(self):
        """Test successful post sending"""
        with patch('src.idempotent_producer.KafkaProducer') as mock_producer:
            # Mock the send method
            mock_future = Mock()
            mock_future.get.return_value = Mock(partition=0, offset=123)
            mock_producer.return_value.send.return_value = mock_future
            
            producer = StreamSocialProducer(KAFKA_CONFIG)
            result = producer.send_post("user1", "Test post")
            
            assert result['status'] == 'success'
            assert 'post_id' in result
            assert result['partition'] == 0
            assert result['offset'] == 123
            
    def test_chaos_monkey_activation(self):
        """Test chaos monkey introduces delays"""
        with patch('src.idempotent_producer.KafkaProducer'):
            producer = StreamSocialProducer(KAFKA_CONFIG, enable_chaos=True)
            
            # Force chaos activation
            producer.chaos.failure_rate = 1.0  # 100% failure rate
            
            start_time = time.time()
            try:
                producer.send_post("user1", "Test post")
            except:
                pass  # Expected to fail during chaos
                
            # Should have introduced some delay
            assert time.time() - start_time > 0.1

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
