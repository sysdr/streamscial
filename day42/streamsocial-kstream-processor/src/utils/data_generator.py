import random
import time
import json
from kafka import KafkaProducer
from src.models.interaction import InteractionEvent

class InteractionDataGenerator:
    """Generate sample interaction events for testing"""
    
    def __init__(self, bootstrap_servers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.interaction_types = ['LIKE', 'SHARE', 'COMMENT', 'VIEW']
    
    def generate_events(self, count=1000, rate=100):
        """Generate events at specified rate (events/sec)"""
        interval = 1.0 / rate
        
        for i in range(count):
            event = InteractionEvent(
                user_id=f"user_{random.randint(1, 100)}",
                post_id=f"post_{random.randint(1, 50)}",
                interaction_type=random.choice(self.interaction_types),
                timestamp=time.time()
            )
            
            self.producer.send(self.topic, value=event.to_json())
            
            if (i + 1) % 100 == 0:
                print(f"Generated {i + 1} events")
            
            time.sleep(interval)
        
        self.producer.flush()
        print(f"Generated {count} events successfully")

if __name__ == '__main__':
    generator = InteractionDataGenerator('localhost:9092', 'user-interactions')
    generator.generate_events(count=1000, rate=100)
