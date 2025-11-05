import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

class IOSProducer:
    def __init__(self, bootstrap_servers=['localhost:9093']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'raw-user-actions-ios'
        
    def generate_event(self):
        actions = ['LIKE', 'COMMENT', 'SHARE', 'FOLLOW']
        users = ['alice123', 'bob456', 'carol789', 'dave012']
        posts = ['p789', 'p234', 'p567', 'p890']
        
        # iOS format: camelCase, Unix timestamp in seconds
        event = {
            'action': random.choice(actions),
            'user': random.choice(users),
            'postId': random.choice(posts),
            'ts': int(time.time())
        }
        
        # Randomly inject invalid events (10% chance)
        if random.random() < 0.1:
            event.pop('user')  # Missing required field
            
        return event
    
    def produce(self, count=100, interval=0.1):
        print(f"📱 iOS Producer: Sending {count} events to {self.topic}")
        for i in range(count):
            event = self.generate_event()
            self.producer.send(self.topic, event)
            print(f"  Sent: {event}")
            time.sleep(interval)
        
        self.producer.flush()
        print(f"✅ iOS Producer: Sent {count} events")
    
    def close(self):
        self.producer.close()

if __name__ == '__main__':
    producer = IOSProducer()
    try:
        producer.produce(count=50)
    finally:
        producer.close()
