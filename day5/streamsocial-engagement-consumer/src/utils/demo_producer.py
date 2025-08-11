#!/usr/bin/env python3

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from config.consumer_config import ConsumerConfig

class DemoProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=ConsumerConfig.KAFKA_BOOTSTRAP_SERVERS.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        self.users = [f"user{i}" for i in range(1, 101)]
        self.posts = [f"post{i}" for i in range(1, 21)]
        self.actions = ["like", "share", "comment"]
        self.comments = [
            "Great post!", "Love this!", "Thanks for sharing",
            "Awesome content", "Very insightful", "Interesting perspective",
            "Well said!", "Couldn't agree more", "Mind blown!", "So true!"
        ]
    
    def generate_engagement_event(self):
        """Generate a random engagement event"""
        action_type = random.choice(self.actions)
        event = {
            "user_id": random.choice(self.users),
            "post_id": random.choice(self.posts),
            "action_type": action_type,
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "source": "demo",
                "ip": f"192.168.1.{random.randint(1, 255)}"
            }
        }
        
        if action_type == "comment":
            event["content"] = random.choice(self.comments)
        
        return event
    
    def start_generating(self, rate_per_second=10, duration_seconds=300):
        """Start generating demo events"""
        print(f"🎬 Starting demo producer: {rate_per_second} events/sec for {duration_seconds}s")
        
        events_sent = 0
        start_time = time.time()
        
        try:
            while time.time() - start_time < duration_seconds:
                batch_start = time.time()
                
                # Send batch of events
                for _ in range(rate_per_second):
                    event = self.generate_engagement_event()
                    
                    self.producer.send('user-engagements', event)
                    events_sent += 1
                    
                    if events_sent % 100 == 0:
                        print(f"📤 Sent {events_sent} events")
                
                # Wait for the rest of the second
                elapsed = time.time() - batch_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
        
        except KeyboardInterrupt:
            print("🛑 Demo producer stopped by user")
        
        finally:
            self.producer.flush()
            self.producer.close()
            print(f"✅ Demo completed. Total events sent: {events_sent}")

if __name__ == "__main__":
    producer = DemoProducer()
    producer.start_generating(rate_per_second=5, duration_seconds=60)
