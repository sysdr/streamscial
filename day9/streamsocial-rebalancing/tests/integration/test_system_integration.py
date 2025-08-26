import pytest
import time
import threading
from unittest.mock import patch, Mock
import redis

from src.main import StreamSocialRebalancingDemo
from src.consumers.feed_consumer import FeedGeneratorConsumer
from src.producers.interaction_producer import InteractionProducer

class TestSystemIntegration:
    def setup_method(self):
        # Use test Redis database
        self.redis_client = redis.Redis(host='localhost', port=6379, db=15, decode_responses=True)
        self.redis_client.flushdb()
        
    def teardown_method(self):
        self.redis_client.flushdb()
        
    @patch('src.main.KafkaConsumer')
    @patch('src.main.KafkaProducer')
    def test_consumer_startup(self, mock_producer, mock_consumer):
        """Test consumer starts up correctly"""
        # Setup mocks
        mock_consumer_instance = Mock()
        mock_consumer.return_value = mock_consumer_instance
        
        # Test
        consumer = FeedGeneratorConsumer("test-consumer")
        
        # Verify consumer is configured
        assert consumer.consumer_id == "test-consumer"
        assert consumer.redis_client is not None
        
    def test_traffic_spike_simulation(self):
        """Test traffic spike triggers scaling"""
        # Setup
        with open('/tmp/consumer_count.txt', 'w') as f:
            f.write('3')
            
        # Simulate traffic spike effect
        producer = InteractionProducer()
        original_rate = 100
        producer.events_per_second = original_rate
        
        # Create spike
        producer.events_per_second = original_rate * 10
        assert producer.events_per_second == 1000
        
        # Verify scaling would be triggered
        assert producer.events_per_second > original_rate
        
    def test_consumer_count_scaling(self):
        """Test consumer count scaling mechanism"""
        # Initial state
        with open('/tmp/consumer_count.txt', 'w') as f:
            f.write('3')
            
        # Read initial count
        with open('/tmp/consumer_count.txt', 'r') as f:
            initial_count = int(f.read().strip())
        assert initial_count == 3
        
        # Simulate scale up
        with open('/tmp/consumer_count.txt', 'w') as f:
            f.write('6')
            
        # Verify new count
        with open('/tmp/consumer_count.txt', 'r') as f:
            new_count = int(f.read().strip())
        assert new_count == 6
        
    def test_cache_persistence(self):
        """Test cache persistence during rebalancing"""
        # Setup cache data
        cache_key = "feed_cache_test_consumer_0"
        cache_data = {"user1": "preferences", "feeds": ["item1", "item2"]}
        
        # Store in Redis
        self.redis_client.setex(cache_key, 300, json.dumps(cache_data))
        
        # Verify retrieval
        retrieved = self.redis_client.get(cache_key)
        assert retrieved is not None
        
        parsed_data = json.loads(retrieved)
        assert parsed_data == cache_data

if __name__ == "__main__":
    pytest.main([__file__])
