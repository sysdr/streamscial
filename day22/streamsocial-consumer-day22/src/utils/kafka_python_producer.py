"""Test producer using kafka-python library."""

import json
import time
import random
from kafka import KafkaProducer

class KafkaPythonTestProducer:
    def __init__(self):
        # Use localhost:9092 for host connections
        bootstrap_servers = 'localhost:9092'
        
        print(f"KafkaPythonTestProducer connecting to: {bootstrap_servers}")
        
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            batch_size=1,
            linger_ms=0
        )
        
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
        
        future = self.producer.send(topic, value=message, key=message['user_id'])
        # Don't wait for confirmation to avoid timeout issues
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
        self.producer.close()

if __name__ == "__main__":
    import sys
    
    producer = KafkaPythonTestProducer()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'load':
        rate = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        duration = int(sys.argv[3]) if len(sys.argv) > 3 else 60
        producer.send_load_test(rate, duration)
    else:
        # Send single test message
        msg = producer.send_notification()
        print(f"Sent test notification: {msg['id']}")
