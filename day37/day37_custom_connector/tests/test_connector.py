"""
Unit tests for connector components
"""
import pytest
import time
from src.connector.social_stream_connector import SocialStreamConnector
from src.connector.source_task import SocialStreamSourceTask
from src.utils.rate_limiter import TokenBucketRateLimiter


def test_connector_initialization():
    """Test connector can be created"""
    connector = SocialStreamConnector()
    assert connector.version() == "1.0.0"
    assert connector.state == "UNASSIGNED"


def test_connector_configuration():
    """Test connector configuration validation"""
    connector = SocialStreamConnector()
    
    valid_config = {
        'connector': {'name': 'test'},
        'platforms': {
            'twitter': {'enabled': True, 'api_base': 'http://api', 'rate_limit': 900}
        },
        'kafka': {'bootstrap_servers': 'localhost:9092', 'topic': 'test'},
        'accounts': [
            {'platform': 'twitter', 'account_id': '123', 'credentials': {'access_token': 'abc'}}
        ]
    }
    
    connector.start(valid_config)
    assert connector.state == "RUNNING"


def test_rate_limiter():
    """Test rate limiter token bucket"""
    limiter = TokenBucketRateLimiter()
    limiter.configure('test', max_requests=10, window_seconds=10)
    
    # Should be able to acquire immediately
    wait_time = limiter.acquire('test', tokens_needed=1)
    assert wait_time == 0.0
    
    # After 10 requests, should need to wait
    for _ in range(9):
        limiter.acquire('test', tokens_needed=1)
    
    wait_time = limiter.acquire('test', tokens_needed=1)
    assert wait_time > 0


def test_source_task_creation():
    """Test source task can be created"""
    task = SocialStreamSourceTask(task_id=0)
    assert task.task_id == 0
    assert not task.running


def test_task_configuration():
    """Test task accepts valid configuration"""
    task = SocialStreamSourceTask(task_id=0)
    
    config = {
        'platform': 'twitter',
        'account_id': 'test_account',
        'credentials': {'access_token': 'test_token'},
        'api_base': 'http://api',
        'rate_limit': 900,
        'poll_interval_ms': 60000,
        'kafka': {
            'bootstrap_servers': 'localhost:9092',
            'topic': 'test-topic',
            'producer_config': {}
        }
    }
    
    task.start(config)
    assert task.running
    assert task.config['platform'] == 'twitter'
    
    task.stop()
    assert not task.running


def test_task_metrics():
    """Test task metrics collection"""
    task = SocialStreamSourceTask(task_id=0)
    
    config = {
        'platform': 'twitter',
        'account_id': 'test_account',
        'credentials': {'access_token': 'test_token'},
        'api_base': 'http://api',
        'rate_limit': 900,
        'poll_interval_ms': 60000,
        'kafka': {
            'bootstrap_servers': 'localhost:9092',
            'topic': 'test-topic',
            'producer_config': {}
        }
    }
    
    task.start(config)
    metrics = task.get_metrics()
    
    assert metrics['task_id'] == 0
    assert metrics['platform'] == 'twitter'
    assert 'metrics' in metrics
    assert 'records_produced' in metrics['metrics']
    
    task.stop()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
