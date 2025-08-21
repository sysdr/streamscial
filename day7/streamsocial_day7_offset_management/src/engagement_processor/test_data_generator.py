import asyncio
import json
import random
import time
from kafka import KafkaProducer
from config.app_config import AppConfig

class TestDataGenerator:
    def __init__(self, config: AppConfig):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def generate_engagement_event(self) -> dict:
        """Generate a realistic engagement event"""
        user_id = f"user_{random.randint(1, 10000)}"
        content_id = f"content_{random.randint(1, 1000)}"
        event_types = ["view", "like", "share", "comment"]
        event_type = random.choice(event_types)
        
        event = {
            "user_id": user_id,
            "event_type": event_type,
            "content_id": content_id,
            "timestamp": int(time.time() * 1000),
            "metadata": {}
        }
        
        if event_type == "view":
            event["metadata"]["duration"] = random.randint(5, 300)  # seconds
        elif event_type == "comment":
            event["metadata"]["comment_length"] = random.randint(10, 200)
            
        return event
    
    async def start_generating(self, events_per_second: int = 10, duration_seconds: int = 60):
        """Generate test events"""
        print(f"Generating {events_per_second} events/sec for {duration_seconds} seconds")
        
        total_events = 0
        end_time = time.time() + duration_seconds
        
        while time.time() < end_time:
            batch_start = time.time()
            
            for _ in range(events_per_second):
                event = self.generate_engagement_event()
                self.producer.send(self.config.kafka.engagement_topic, event)
                total_events += 1
            
            # Maintain target rate
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
                
            if total_events % 100 == 0:
                print(f"Generated {total_events} events")
        
        self.producer.flush()
        self.producer.close()
        print(f"Generation complete. Total events: {total_events}")

if __name__ == "__main__":
    config = AppConfig()
    generator = TestDataGenerator(config)
    asyncio.run(generator.start_generating(events_per_second=50, duration_seconds=120))
