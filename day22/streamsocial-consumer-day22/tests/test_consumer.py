"""Tests for low-latency consumer."""

import pytest
import asyncio
import json
import time
from unittest.mock import Mock, patch
from src.consumer.low_latency_consumer import LowLatencyConsumer, NotificationProcessor

@pytest.mark.asyncio
async def test_notification_processor():
    """Test notification processing latency."""
    processor = NotificationProcessor()
    
    test_message = {
        'id': 'test_001',
        'type': 'critical',
        'user_id': 'user_123',
        'timestamp': time.time()
    }
    
    start_time = time.time()
    result = await processor.process_notification(test_message)
    processing_time = (time.time() - start_time) * 1000  # ms
    
    assert result is True
    assert processing_time < 30  # Should be under 30ms

@pytest.mark.asyncio 
async def test_processing_latency_by_type():
    """Test that different notification types have appropriate latency."""
    processor = NotificationProcessor()
    
    types_and_limits = [
        ('critical', 10),   # Should be under 10ms
        ('important', 20),  # Should be under 20ms
        ('standard', 30)    # Should be under 30ms
    ]
    
    for notif_type, limit_ms in types_and_limits:
        message = {
            'id': f'test_{notif_type}',
            'type': notif_type,
            'user_id': 'user_test',
            'timestamp': time.time()
        }
        
        start_time = time.time()
        result = await processor.process_notification(message)
        processing_time = (time.time() - start_time) * 1000
        
        assert result is True
        assert processing_time < limit_ms, f"{notif_type} took {processing_time}ms, limit was {limit_ms}ms"

def test_consumer_stats():
    """Test consumer statistics tracking."""
    consumer = LowLatencyConsumer()
    stats = consumer.get_stats()
    
    assert 'messages_processed' in stats
    assert 'average_latency' in stats
    assert 'max_latency' in stats
    assert 'errors' in stats
    assert stats['messages_processed'] == 0

@pytest.mark.asyncio
async def test_mock_message_processing():
    """Test message processing with mocked Kafka message."""
    consumer = LowLatencyConsumer()
    
    # Mock Kafka message
    mock_msg = Mock()
    mock_msg.topic.return_value = 'test-topic'
    mock_msg.value.return_value = json.dumps({
        'id': 'test_msg',
        'type': 'critical',
        'user_id': 'user_test',
        'timestamp': time.time()
    }).encode('utf-8')
    
    await consumer._process_message(mock_msg)
    
    stats = consumer.get_stats()
    assert stats['messages_processed'] == 1
