import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

class AndroidProducer:
    def __init__(self, bootstrap_servers=['localhost:9093']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'raw-user-actions-android'
        
    def generate_event(self):
        actions = ['like', 'comment', 'share', 'follow']
        users = ['user_alice', 'user_bob', 'user_carol', 'user_dave']
        posts = ['post_123', 'post_456', 'post_789', 'post_012']
        
        # Android format: snake_case, ISO timestamp
        event = {
            'event_type': random.choice(actions),
            'userId': random.choice(users),
            'post_id': random.choice(posts),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        # Randomly inject invalid events (10% chance)
        if random.random() < 0.1:
            event.pop('post_id')  # Missing required field
            
        return event
    
    def produce(self, count=100, interval=0.1):
        print(f"🤖 Android Producer: Sending {count} events to {self.topic}")
        for i in range(count):
            event = self.generate_event()
            self.producer.send(self.topic, event)
            print(f"  Sent: {event}")
            time.sleep(interval)
        
        self.producer.flush()
        print(f"✅ Android Producer: Sent {count} events")
    
    def close(self):
        self.producer.close()

if __name__ == '__main__':
    producer = AndroidProducer()
    try:
        producer.produce(count=50)
    finally:
        producer.close()
