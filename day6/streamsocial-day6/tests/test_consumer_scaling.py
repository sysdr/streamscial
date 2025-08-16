import pytest
import asyncio
import time
from src.utils.consumer_manager import ConsumerGroupManager
from src.producers.activity_producer import ActivityProducer

@pytest.mark.asyncio
async def test_consumer_scaling():
    """Test consumer group scaling functionality"""
    manager = ConsumerGroupManager("test-scaling-group")
    
    try:
        # Test scaling up
        await manager.scale_to(3)
        assert len(manager.consumers) == 3
        
        # Wait for stabilization
        await asyncio.sleep(10)
        
        # Test scaling down
        await manager.scale_to(1)
        assert len(manager.consumers) == 1
        
    finally:
        await manager.shutdown_all()

@pytest.mark.asyncio
async def test_activity_production():
    """Test activity producer functionality"""
    producer = ActivityProducer()
    
    try:
        # Produce small batch
        await producer.produce_activities(rate_per_second=10, duration_seconds=5)
        
    finally:
        producer.close()

def test_consumer_metrics():
    """Test consumer metrics collection"""
    from src.consumers.feed_consumer import ConsumerMetrics
    
    metrics = ConsumerMetrics("test-consumer", [0, 1, 2])
    assert metrics.consumer_id == "test-consumer"
    assert metrics.assigned_partitions == [0, 1, 2]
    assert metrics.messages_processed == 0
