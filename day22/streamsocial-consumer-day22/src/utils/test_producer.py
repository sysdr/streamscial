"""Test producer for generating notification messages."""

import json
import time
import random
import os
from confluent_kafka import Producer
from config.consumer_config import ConsumerConfig

class TestProducer:
    def __init__(self):
        # Always use localhost:9092 for host connections
        # The confluent-kafka library seems to have issues with Docker service names
        bootstrap_servers = 'localhost:9092'
        
        print(f"TestProducer connecting to: {bootstrap_servers}")
        
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'compression.type': 'none',
            'linger.ms': 0,  # Send immediately
            'batch.size': 1   # No batching
        })
        
        self.notification_types = ['critical', 'important', 'standard']
        self.message_count = 0
    
    def generate_notification(self, notification_type='random'):
        """Generate a realistic notification message."""
        if notification_type == 'random':
            notification_type = random.choice(self.notification_types)
        
        self.message_count += 1
        
        return {
            'id': f'notif_{self.message_count}',
            'type': notification_type,
            'user_id': f'user_{random.randint(1, 10000)}',
            'title': f'StreamSocial {notification_type.title()} Alert',
            'message': f'You have a new {notification_type} notification',
            'timestamp': time.time(),
            'priority': {'critical': 1, 'important': 2, 'standard': 3}[notification_type]
        }
    
    def send_notification(self, notification_type='random', topic=None):
        """Send a single notification."""
        message = self.generate_notification(notification_type)
        
        if not topic:
            topic = f"{notification_type}-notifications"
        
        self.producer.produce(
            topic=topic,
            value=json.dumps(message),
            key=message['user_id']
        )
        self.producer.flush()
        return message
    
    def send_load_test(self, messages_per_second=100, duration_seconds=60):
        """Send sustained load for testing."""
        print(f"Starting load test: {messages_per_second} msg/sec for {duration_seconds}s")
        
        interval = 1.0 / messages_per_second
        end_time = time.time() + duration_seconds
        
        while time.time() < end_time:
            start = time.time()
            
            # Send message
            self.send_notification()
            
            # Sleep to maintain rate
            elapsed = time.time() - start
            sleep_time = max(0, interval - elapsed)
            time.sleep(sleep_time)
        
        print(f"Load test completed. Sent {self.message_count} messages")

if __name__ == "__main__":
    import sys
    
    producer = TestProducer()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'load':
        rate = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        duration = int(sys.argv[3]) if len(sys.argv) > 3 else 60
        producer.send_load_test(rate, duration)
    else:
        # Send single test message
        msg = producer.send_notification()
        print(f"Sent test notification: {msg['id']}")
