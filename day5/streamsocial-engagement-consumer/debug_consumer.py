#!/usr/bin/env python3

import json
import time
import structlog
from kafka import KafkaConsumer
from config.consumer_config import ConsumerConfig
from src.models.engagement import EngagementEvent
from src.utils.cache import CacheManager
from src.utils.metrics import MetricsCollector

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

def debug_consumer():
    """Debug version of the consumer to identify issues"""
    print("🔍 Starting debug consumer...")
    
    try:
        # Create consumer
        consumer = KafkaConsumer(
            'user-engagements',
            **ConsumerConfig.get_kafka_config()
        )
        
        print("✅ Consumer created successfully")
        print(f"📊 Consumer config: {ConsumerConfig.get_kafka_config()}")
        
        # Create cache manager and metrics collector
        cache_manager = CacheManager()
        metrics_collector = MetricsCollector()
        
        print("✅ Cache manager and metrics collector created")
        
        message_count = 0
        start_time = time.time()
        
        print("🔍 Starting to poll for messages...")
        
        # Poll for messages
        while message_count < 10:  # Process up to 10 messages for debugging
            print(f"🔍 Polling for messages (attempt {message_count + 1})...")
            
            messages = consumer.poll(timeout_ms=5000)
            
            if messages:
                print(f"✅ Found {len(messages)} message partitions")
                
                for topic_partition, partition_messages in messages.items():
                    print(f"   Processing partition {topic_partition}: {len(partition_messages)} messages")
                    
                    for message in partition_messages:
                        try:
                            print(f"📥 Processing message: {message.value[:100]}...")
                            
                            # Deserialize message
                            engagement_event = EngagementEvent.from_kafka_message(message.value)
                            print(f"✅ Message deserialized: {engagement_event}")
                            
                            # Update engagement statistics in cache
                            stats = cache_manager.update_engagement_stats(
                                engagement_event.post_id,
                                engagement_event.action_type
                            )
                            print(f"✅ Cache updated: {stats}")
                            
                            # Record metrics
                            metrics_collector.record_engagement_event(engagement_event.action_type)
                            print(f"✅ Metrics recorded for action: {engagement_event.action_type}")
                            
                            message_count += 1
                            
                            if message_count >= 10:
                                break
                                
                        except Exception as e:
                            print(f"❌ Error processing message: {str(e)}")
                            import traceback
                            traceback.print_exc()
                            break
                    
                    if message_count >= 10:
                        break
            else:
                print("⚠️  No messages found in this poll")
            
            time.sleep(1)
        
        print(f"✅ Debug completed. Processed {message_count} messages in {time.time() - start_time:.2f} seconds")
        
        # Get final metrics
        metrics = metrics_collector.get_system_metrics()
        print(f"📊 Final metrics: {metrics}")
        
        consumer.close()
        
    except Exception as e:
        print(f"❌ Debug consumer failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_consumer()
