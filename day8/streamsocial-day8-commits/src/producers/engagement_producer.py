import asyncio
import json
import random
from datetime import datetime
from confluent_kafka import Producer
from typing import List
import uuid

from ..models.engagement import EngagementEvent

class EngagementProducer:
    def __init__(self, config: dict):
        self.config = config
        self.producer = Producer({
            'bootstrap.servers': config['bootstrap_servers'],
            'acks': 'all',
            'retries': 3,
            'batch.size': 16384,
            'linger.ms': 10,
        })
    
    def delivery_callback(self, err, msg):
        """Delivery callback for produced messages"""
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
    
    def generate_sample_engagement(self) -> EngagementEvent:
        """Generate sample engagement for testing"""
        engagement_types = ['like', 'share', 'comment', 'follow', 'view']
        
        return EngagementEvent(
            user_id=f"user_{random.randint(1000, 9999)}",
            content_id=f"content_{random.randint(100, 999)}",
            engagement_type=random.choice(engagement_types),
            timestamp=datetime.utcnow(),
            metadata={
                'device': random.choice(['mobile', 'web', 'tablet']),
                'location': random.choice(['US', 'UK', 'CA', 'AU']),
                'session_id': str(uuid.uuid4())
            },
            event_id=str(uuid.uuid4())
        )
    
    def produce_engagement(self, engagement: EngagementEvent):
        """Produce single engagement event"""
        try:
            self.producer.produce(
                topic=self.config['topic'],
                key=engagement.user_id,
                value=engagement.to_json(),
                callback=self.delivery_callback
            )
            self.producer.poll(0)  # Trigger delivery callbacks
        except Exception as e:
            print(f"Failed to produce engagement: {e}")
    
    async def produce_sample_data(self, count: int = 100, delay: float = 0.1):
        """Produce sample engagement data for testing"""
        print(f"Producing {count} sample engagements...")
        
        for i in range(count):
            engagement = self.generate_sample_engagement()
            self.produce_engagement(engagement)
            
            if i % 10 == 0:
                print(f"Produced {i + 1}/{count} engagements")
            
            await asyncio.sleep(delay)
        
        # Wait for all messages to be delivered
        self.producer.flush()
        print(f"Completed producing {count} engagements")
