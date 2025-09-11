import pytest
import time
from unittest.mock import Mock, patch
from src.producer.adaptive_producer import AdaptiveKafkaProducer
from src.models.post_event import PostEvent

class TestAdaptiveKafkaProducer:
    def test_producer_initialization(self):
        with patch('src.producer.adaptive_producer.KafkaProducer'):
            producer = AdaptiveKafkaProducer('low_latency')
            assert producer.config_name == 'low_latency'
            assert 'batch.size' in producer.config
    
    def test_message_sending(self):
        with patch('src.producer.adaptive_producer.KafkaProducer') as mock_producer:
            mock_instance = Mock()
            mock_producer.return_value = mock_instance
            mock_future = Mock()
            mock_instance.send.return_value = mock_future
            
            producer = AdaptiveKafkaProducer('adaptive')
            post_event = PostEvent(
                user_id="test_user",
                post_id="test_post",
                content="Test content",
                timestamp=int(time.time() * 1000),
                region="us-east"
            )
            
            result = producer.send_message('test_topic', post_event)
            assert result == True
            mock_instance.send.assert_called_once()
    
    def test_config_adaptation(self):
        with patch('src.producer.adaptive_producer.KafkaProducer'):
            producer = AdaptiveKafkaProducer('adaptive')
            
            # Simulate high traffic scenario
            producer.metrics['messages_per_second'] = 150000
            producer.metrics['buffer_usage'] = 30
            
            original_batch_size = producer.config['batch.size']
            producer._adjust_configuration()
            
            # Should increase batch size for high traffic
            assert producer.config['batch.size'] >= original_batch_size
    
    def test_buffer_usage_estimation(self):
        with patch('src.producer.adaptive_producer.KafkaProducer'):
            producer = AdaptiveKafkaProducer('adaptive')
            producer.metrics['messages_per_second'] = 10000
            
            usage = producer._estimate_buffer_usage()
            assert 0 <= usage <= 100
            assert isinstance(usage, float)

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
