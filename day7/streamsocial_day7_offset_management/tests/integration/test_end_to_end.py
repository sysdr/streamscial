import pytest
import asyncio
import json
import time
from kafka import KafkaProducer
from config.app_config import AppConfig
from src.engagement_processor.processor import EngagementProcessor

@pytest.mark.asyncio
async def test_end_to_end_processing():
    """Test complete end-to-end processing flow"""
    config = AppConfig()
    
    # Generate test events
    producer = KafkaProducer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    test_events = [
        {
            "user_id": f"test_user_{i}",
            "event_type": "view",
            "content_id": f"content_{i % 10}",
            "timestamp": int(time.time() * 1000),
            "metadata": {"duration": 30}
        }
        for i in range(50)
    ]
    
    # Send test events
    for event in test_events:
        producer.send(config.kafka.engagement_topic, event)
    
    producer.flush()
    producer.close()
    
    # Initialize processor
    processor = EngagementProcessor(config)
    await processor.initialize()
    
    # Process for a short time
    processing_task = asyncio.create_task(processor.start_processing())
    await asyncio.sleep(10)  # Process for 10 seconds
    
    await processor.stop()
    processing_task.cancel()
    
    # Verify metrics were created
    async with processor.postgres_pool.acquire() as conn:
        metrics_count = await conn.fetchval(
            "SELECT COUNT(*) FROM engagement_metrics WHERE metric_type = 'view_count'"
        )
        
        assert metrics_count > 0, "No metrics were processed"
        print(f"✅ Processed {metrics_count} view count metrics")
