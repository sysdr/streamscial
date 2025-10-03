#!/usr/bin/env python3
"""Simple test consumer to verify Kafka connection."""

import os
import json
import time
from confluent_kafka import Consumer

# Set environment variables explicitly
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'kafka:9092'
os.environ['KAFKA_SERVERS'] = 'kafka:9092'

def main():
    print("Starting simple test consumer...")
    
    # Create consumer with explicit configuration
    config = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'test-simple-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    print(f"Consumer config: {config}")
    consumer = Consumer(config)
    
    # Subscribe to topics
    topics = ['critical-notifications', 'important-notifications', 'standard-notifications']
    consumer.subscribe(topics)
    print(f"Subscribed to topics: {topics}")
    
    message_count = 0
    start_time = time.time()
    
    try:
        while time.time() - start_time < 30:  # Run for 30 seconds
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                print("No message received (timeout)")
                continue
                
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            # Process message
            message_count += 1
            message_data = json.loads(msg.value().decode('utf-8'))
            print(f"Message {message_count} from {msg.topic()}: {message_data.get('id', 'unknown')}")
            
            # Commit message
            consumer.commit(asynchronous=False)
            
    except KeyboardInterrupt:
        print("Received interrupt signal")
    finally:
        consumer.close()
        print(f"Consumer closed. Processed {message_count} messages.")

if __name__ == "__main__":
    main()
