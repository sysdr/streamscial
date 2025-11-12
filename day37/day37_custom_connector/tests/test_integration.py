"""
Integration tests - test full connector flow
"""
import pytest
import time
import threading
from src.connector.social_stream_connector import load_connector_from_config
from src.connector.source_task import SocialStreamSourceTask


def test_connector_loads_config():
    """Test connector loads from config file"""
    connector = load_connector_from_config('config/connector_config.yaml')
    assert connector.state == "RUNNING"
    assert connector.version() == "1.0.0"


def test_connector_creates_tasks():
    """Test connector creates correct number of tasks"""
    connector = load_connector_from_config('config/connector_config.yaml')
    task_configs = connector.task_configs(max_tasks=10)
    
    # Should have one task per account
    assert len(task_configs) == 2  # twitter + linkedin from config
    
    # Each task should have required fields
    for config in task_configs:
        assert 'platform' in config
        assert 'account_id' in config
        assert 'credentials' in config
        assert 'kafka' in config


def test_task_produces_records():
    """Test task can poll and produce records"""
    connector = load_connector_from_config('config/connector_config.yaml')
    task_configs = connector.task_configs(max_tasks=1)
    
    task = SocialStreamSourceTask(task_id=0)
    task.start(task_configs[0])
    
    # Poll once
    records = task.poll()
    
    # Should get some records (mock API returns 5-15 posts)
    assert len(records) > 0
    assert len(records) <= 15
    
    # Verify record structure
    first_record = records[0]
    assert first_record.topic == 'social-posts'
    assert first_record.source_partition is not None
    assert first_record.source_offset is not None
    
    # Check metrics
    metrics = task.get_metrics()
    assert metrics['metrics']['records_produced'] > 0
    assert metrics['metrics']['api_calls'] == 1
    
    task.stop()


def test_rate_limiting_works():
    """Test rate limiter prevents excessive API calls"""
    connector = load_connector_from_config('config/connector_config.yaml')
    task_configs = connector.task_configs(max_tasks=1)
    
    # Reduce rate limit for testing
    task_configs[0]['rate_limit'] = 2  # Only 2 requests allowed
    
    task = SocialStreamSourceTask(task_id=0)
    task.start(task_configs[0])
    
    # Make 3 polls quickly
    for _ in range(3):
        task.poll()
        
    # Should have rate limit waits
    metrics = task.get_metrics()
    assert metrics['metrics']['rate_limit_waits'] > 0
    
    task.stop()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
