import pytest
import json
from unittest.mock import Mock, patch
from src.main.python.geo_monitor import GeoPartitionMonitor
from src.main.python.geo_producer import GeoAwareProducer

class TestGeoPartitionMonitor:
    def setup_method(self):
        self.monitor = GeoPartitionMonitor()
    
    def test_partition_health_api(self):
        with self.monitor.app.test_client() as client:
            response = client.get('/api/partition-health')
            assert response.status_code == 200
            data = json.loads(response.data)
            assert len(data) == 12  # Should have 12 partitions
            
            # Check structure
            for partition, info in data.items():
                assert 'current_health' in info
                assert 'avg_latency' in info
                assert 'message_count' in info
                assert 'status' in info
    
    def test_region_distribution_api(self):
        with self.monitor.app.test_client() as client:
            response = client.get('/api/region-distribution')
            assert response.status_code == 200
            data = json.loads(response.data)
            assert isinstance(data, dict)

class TestGeoAwareProducer:
    def setup_method(self):
        with patch('src.main.python.geo_producer.KafkaProducer'):
            self.producer = GeoAwareProducer()
    
    def test_send_geo_message(self):
        with patch.object(self.producer.producer, 'send') as mock_send:
            mock_send.return_value = Mock()
            
            future = self.producer.send_geo_message("user123", "US_EAST", "test content")
            
            mock_send.assert_called_once()
            args, kwargs = mock_send.call_args
            
            # Check topic (first positional argument)
            assert args[0] == 'streamsocial-posts'
            
            # Check key format (from kwargs)
            assert kwargs['key'] == "user:US_EAST:user123"
            
            # Check message structure (from kwargs)
            message = kwargs['value']
            assert message['user_id'] == 'user123'
            assert message['region'] == 'US_EAST'
            assert message['content'] == 'test content'
