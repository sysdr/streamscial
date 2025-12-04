import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from src.models import UserAction, ActionType
from config.settings import settings

class ReputationEventProducer:
    """Produces user action events with realistic patterns"""
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        self.running = False
        
        # Generate pool of users
        self.users = [f"user_{i:05d}" for i in range(10000)]
        self.posts = [f"post_{i:06d}" for i in range(50000)]
        
    def generate_action(self) -> UserAction:
        """Generate realistic user action"""
        # Weighted action distribution (likes more common)
        action_weights = {
            ActionType.LIKE: 0.7,
            ActionType.COMMENT: 0.2,
            ActionType.SHARE: 0.1
        }
        
        action_type = random.choices(
            list(action_weights.keys()),
            weights=list(action_weights.values())
        )[0]
        
        return UserAction(
            user_id=random.choice(self.users),
            action_type=action_type,
            post_id=random.choice(self.posts),
            timestamp=datetime.utcnow()
        )
    
    def produce_events(self, duration_seconds: int = 60, rate_per_second: int = 1000):
        """Produce events at specified rate"""
        self.running = True
        start_time = time.time()
        events_produced = 0
        
        print(f"Starting event production: {rate_per_second} events/sec for {duration_seconds}s")
        
        try:
            while self.running and (time.time() - start_time) < duration_seconds:
                batch_start = time.time()
                
                for _ in range(rate_per_second):
                    action = self.generate_action()
                    
                    self.producer.send(
                        settings.user_actions_topic,
                        key=action.user_id,
                        value=action.model_dump(mode='json')
                    )
                    events_produced += 1
                
                # Rate limiting
                elapsed = time.time() - batch_start
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                
                if events_produced % 10000 == 0:
                    print(f"Produced {events_produced} events...")
                    
        except KeyboardInterrupt:
            print("\nStopping producer...")
        finally:
            self.producer.flush()
            self.producer.close()
            print(f"Total events produced: {events_produced}")

if __name__ == "__main__":
    producer = ReputationEventProducer()
    producer.produce_events(duration_seconds=300, rate_per_second=500)

