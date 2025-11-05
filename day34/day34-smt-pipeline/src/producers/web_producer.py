import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

class WebProducer:
    def __init__(self, bootstrap_servers=['localhost:9093']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'raw-user-actions-web'
        
    def generate_event(self):
        reactions = ['like', 'love', 'haha', 'wow']
        users = ['web_alice', 'web_bob', 'web_carol', 'web_dave']
        targets = ['target_a1', 'target_b2', 'target_c3', 'target_d4']
        
        # Web format: mixed conventions, Unix timestamp in milliseconds
        event = {
            'type': 'reaction',
            'user_id': random.choice(users),
            'target': random.choice(targets),
            'reaction_kind': random.choice(reactions),
            'occurred_at': int(time.time() * 1000)
        }
        
        # Randomly inject invalid events (10% chance)
        if random.random() < 0.1:
            event.pop('user_id')  # Missing required field
            
        return event
    
    def produce(self, count=100, interval=0.1):
        print(f"🌐 Web Producer: Sending {count} events to {self.topic}")
        for i in range(count):
            event = self.generate_event()
            self.producer.send(self.topic, event)
            print(f"  Sent: {event}")
            time.sleep(interval)
        
        self.producer.flush()
        print(f"✅ Web Producer: Sent {count} events")
    
    def close(self):
        self.producer.close()

if __name__ == '__main__':
    producer = WebProducer()
    try:
        producer.produce(count=50)
    finally:
        producer.close()
