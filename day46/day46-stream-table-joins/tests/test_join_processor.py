import pytest
import json
import time
import threading
import sys
import os
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# Add src directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(project_root, 'src')
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from processors.join_processor import StreamTableJoinProcessor

def test_stream_table_join():
    """Test that actions are enriched with profile data"""
    
    # Setup
    bootstrap_servers = 'localhost:9092'
    
    # Start the join processor in a background thread
    processor = StreamTableJoinProcessor(bootstrap_servers=bootstrap_servers)
    processor_thread = threading.Thread(target=processor.start, daemon=True)
    processor_thread.start()
    
    # Wait for processor to initialize (materialize KTable)
    print("Waiting for processor to initialize...")
    time.sleep(5)
    
    # Create test profile
    profile_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    test_profile = {
        'user_id': 'test_user_999',
        'age': 30,
        'city': 'TestCity',
        'tier': 'premium'
    }
    
    profile_producer.send('user-profiles', key='test_user_999', value=test_profile)
    profile_producer.flush()
    profile_producer.close()
    
    # Wait for KTable to update
    print("Waiting for KTable to update...")
    time.sleep(3)
    
    # Create test action
    action_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    test_action = {
        'user_id': 'test_user_999',
        'action_type': 'post_liked',
        'target_id': 'post_123',
        'timestamp': int(time.time() * 1000)
    }
    
    action_producer.send('user-actions', key='test_user_999', value=test_action)
    action_producer.flush()
    action_producer.close()
    
    # Wait for processing
    print("Waiting for join processing...")
    time.sleep(2)
    
    # Consume enriched result
    consumer = KafkaConsumer(
        'enriched-actions',
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',  # Changed from 'latest' to 'earliest' to catch our message
        consumer_timeout_ms=10000,
        group_id='test-consumer-group'
    )
    
    # Find our enriched event
    found = False
    start_time = time.time()
    timeout = 10  # 10 seconds timeout
    
    print("Consuming enriched events...")
    for message in consumer:
        if time.time() - start_time > timeout:
            break
        if message.value.get('user_id') == 'test_user_999':
            enriched = message.value
            assert enriched['user_age'] == 30
            assert enriched['user_city'] == 'TestCity'
            assert enriched['user_tier'] == 'premium'
            assert enriched['action_type'] == 'post_liked'
            assert 'enrichment_latency_ms' in enriched
            found = True
            break
    
    consumer.close()
    
    assert found, "Enriched event not found"
    print("✓ Stream-table join test passed")

if __name__ == '__main__':
    test_stream_table_join()
