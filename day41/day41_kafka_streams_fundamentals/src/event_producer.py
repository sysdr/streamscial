"""
User Interaction Event Producer
Simulates real-time user interactions (likes, comments, shares, views)
"""

import json
import random
import time
from confluent_kafka import Producer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InteractionProducer:
    """Produces realistic user interaction events"""
    
    INTERACTION_TYPES = ['VIEW', 'LIKE', 'COMMENT', 'SHARE']
    CONTENT_IDS = [f'post_{i}' for i in range(1, 51)]  # 50 pieces of content
    USER_IDS = [f'user_{i}' for i in range(1, 201)]  # 200 users
    
    def __init__(self, bootstrap_servers: str):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'interaction-producer'
        })
    
    def generate_interaction(self) -> dict:
        """Generate realistic interaction event"""
        # Weighted probability for interaction types
        interaction_type = random.choices(
            self.INTERACTION_TYPES,
            weights=[60, 25, 10, 5],  # Views most common, shares rarest
            k=1
        )[0]
        
        # Some content is more popular (power law distribution)
        content_id = random.choices(
            self.CONTENT_IDS,
            weights=[100 / (i + 1) for i in range(len(self.CONTENT_IDS))],
            k=1
        )[0]
        
        return {
            'content_id': content_id,
            'user_id': random.choice(self.USER_IDS),
            'interaction_type': interaction_type,
            'timestamp': time.time()
        }
    
    def delivery_callback(self, err, msg):
        """Callback for delivery reports"""
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")
    
    def produce_events(self, events_per_second: int = 100, duration: int = 300):
        """Produce events at specified rate"""
        logger.info(f"Producing {events_per_second} events/sec for {duration} seconds")
        
        start_time = time.time()
        total_events = 0
        
        while time.time() - start_time < duration:
            batch_start = time.time()
            
            for _ in range(events_per_second):
                event = self.generate_interaction()
                
                self.producer.produce(
                    'user-interactions',
                    key=event['content_id'].encode('utf-8'),
                    value=json.dumps(event).encode('utf-8'),
                    callback=self.delivery_callback
                )
                
                total_events += 1
            
            self.producer.poll(0)
            
            # Sleep to maintain rate
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            
            if total_events % 1000 == 0:
                logger.info(f"Produced {total_events} events")
        
        self.producer.flush()
        logger.info(f"Completed: {total_events} total events produced")


if __name__ == '__main__':
    producer = InteractionProducer('localhost:9092')
    producer.produce_events(events_per_second=100, duration=300)
