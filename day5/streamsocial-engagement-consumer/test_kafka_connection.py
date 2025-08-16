#!/usr/bin/env python3

import json
import time
from kafka import KafkaConsumer, KafkaProducer
from config.consumer_config import ConsumerConfig

def test_kafka_connection():
    """Test Kafka producer and consumer connection"""
    print("🔍 Testing Kafka connection...")
    
    try:
        # Test producer
        print("📤 Testing producer...")
        producer = KafkaProducer(
            bootstrap_servers=ConsumerConfig.KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send test message
        test_message = {
            "user_id": "test_user",
            "post_id": "test_post",
            "action_type": "like",
            "timestamp": "2025-08-11T13:30:00.000000"
        }
        
        producer.send('user-engagements', test_message)
        producer.flush()
        print("✅ Producer test message sent successfully")
        
        # Test consumer
        print("📥 Testing consumer...")
        consumer = KafkaConsumer(
            'user-engagements',
            **ConsumerConfig.get_kafka_config()
        )
        
        print("✅ Consumer created successfully")
        print(f"📊 Consumer config: {ConsumerConfig.get_kafka_config()}")
        
        # Try to poll for messages
        print("🔍 Polling for messages...")
        messages = consumer.poll(timeout_ms=5000)
        
        if messages:
            print(f"✅ Found {len(messages)} message partitions")
            for tp, msgs in messages.items():
                print(f"   Partition {tp}: {len(msgs)} messages")
        else:
            print("⚠️  No messages found (this might be normal)")
        
        consumer.close()
        producer.close()
        print("✅ Kafka connection test completed successfully")
        
    except Exception as e:
        print(f"❌ Kafka connection test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_kafka_connection()
