import pytest
import asyncio
import time
import threading
from unittest.mock import patch
from src.producer.adaptive_producer import AdaptiveKafkaProducer
from src.utils.load_simulator import LoadSimulator
from src.models.post_event import PostEvent

class TestFullPipeline:
    @pytest.fixture
    def producer(self):
        # Mock producer for integration tests
        with patch('src.producer.adaptive_producer.KafkaProducer'):
            producer = AdaptiveKafkaProducer('adaptive')
            yield producer
            producer.close()
    
    def test_load_simulation_steady(self, producer):
        simulator = LoadSimulator(producer)
        
        # Run short simulation
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        message_count = loop.run_until_complete(
            simulator.simulate_traffic_pattern(100, 5, 'steady')
        )
        
        assert message_count > 0
        assert message_count <= 500  # 100 msg/s * 5s
    
    def test_benchmark_throughput(self, producer):
        simulator = LoadSimulator(producer)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        results = loop.run_until_complete(
            simulator.benchmark_throughput(1000, 10)
        )
        
        assert 'target_rate' in results
        assert 'actual_rate' in results
        assert 'success_rate' in results
        assert results['target_rate'] == 1000
    
    def test_post_event_serialization(self):
        post = PostEvent(
            user_id="test_user",
            post_id="test_post",
            content="Test content",
            timestamp=int(time.time() * 1000),
            region="us-east"
        )
        
        json_str = post.to_json()
        assert isinstance(json_str, str)
        
        recreated_post = PostEvent.from_json(json_str)
        assert recreated_post.user_id == post.user_id
        assert recreated_post.content == post.content

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
