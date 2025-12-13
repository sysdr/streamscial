import json
import time
import random
from confluent_kafka import Producer
from datetime import datetime

class EventGenerator:
    """Generate synthetic user interaction events"""
    
    def __init__(self, bootstrap_servers: str):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'acks': '1'
        })
        
        self.users = [f"user_{i:04d}" for i in range(1, 101)]
        self.event_types = ['like', 'comment', 'share']
        self.event_weights = [0.6, 0.3, 0.1]  # Likes most common
        
    def generate_event(self) -> dict:
        """Generate random user interaction event"""
        return {
            'user_id': random.choice(self.users),
            'type': random.choices(self.event_types, self.event_weights)[0],
            'timestamp': time.time(),
            'post_id': f"post_{random.randint(1, 1000)}"
        }
        
    def publish_events(self, count: int, rate_per_second: int = 100):
        """Publish events at specified rate"""
        print(f"[GENERATOR] Publishing {count} events at {rate_per_second}/sec")
        
        interval = 1.0 / rate_per_second
        
        for i in range(count):
            event = self.generate_event()
            
            self.producer.produce(
                'user-interactions',
                key=event['user_id'].encode('utf-8'),
                value=json.dumps(event).encode('utf-8')
            )
            
            if (i + 1) % 100 == 0:
                self.producer.flush()
                print(f"[GENERATOR] Published {i + 1}/{count} events")
                
            time.sleep(interval)
            
        self.producer.flush()
        print(f"[GENERATOR] Completed: {count} events published")

if __name__ == '__main__':
    generator = EventGenerator('localhost:9092')
    print("[GENERATOR] Starting continuous event generation...")
    print("[GENERATOR] Generating events at 50/sec (press Ctrl+C to stop)")
    
    # Run continuously
    interval = 1.0 / 50  # 50 events per second
    event_count = 0
    
    try:
        while True:
            event = generator.generate_event()
            generator.producer.produce(
                'user-interactions',
                key=event['user_id'].encode('utf-8'),
                value=json.dumps(event).encode('utf-8')
            )
            
            event_count += 1
            if event_count % 100 == 0:
                generator.producer.flush()
                print(f"[GENERATOR] Published {event_count} events (continuing...)")
                
            time.sleep(interval)
    except KeyboardInterrupt:
        generator.producer.flush()
        print(f"\n[GENERATOR] Stopped. Total events published: {event_count}")
