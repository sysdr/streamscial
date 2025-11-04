"""
Test data producer for hashtag sink connector
Generates realistic hashtag data for testing
"""
import json
import time
import random
from datetime import datetime, timedelta
from typing import List
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

class HashtagDataProducer:
    """Produces test hashtag data to Kafka"""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = topic
        
        # Popular hashtags for realistic data
        self.popular_hashtags = [
            'python', 'kafka', 'streaming', 'analytics', 'bigdata',
            'machinelearning', 'ai', 'tech', 'coding', 'datascience',
            'cloud', 'kubernetes', 'docker', 'microservices', 'api',
            'javascript', 'react', 'nodejs', 'mongodb', 'postgresql',
            'aws', 'azure', 'gcp', 'devops', 'cicd'
        ]
        
    def generate_hashtag_record(self) -> dict:
        """Generate a single hashtag record"""
        hashtag = random.choice(self.popular_hashtags)
        count = random.randint(1, 1000)
        
        now = datetime.now()
        window_start = now - timedelta(minutes=5)
        window_end = now
        
        return {
            'hashtag': hashtag,
            'count': count,
            'window_start': window_start.isoformat(),
            'window_end': window_end.isoformat(),
            'timestamp': now.isoformat()
        }
        
    def produce_batch(self, batch_size: int = 50):
        """Produce a batch of hashtag records"""
        for _ in range(batch_size):
            record = self.generate_hashtag_record()
            self.producer.send(self.topic, record)
            
        self.producer.flush()
        print(f"Produced {batch_size} hashtag records")
        
    def run_continuous(self, interval_seconds: int = 5, batch_size: int = 20):
        """Run continuous data production"""
        print(f"Starting continuous production to topic: {self.topic}")
        
        try:
            while True:
                self.produce_batch(batch_size)
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("Stopping data production...")
        finally:
            self.producer.close()

if __name__ == '__main__':
    producer = HashtagDataProducer(
        bootstrap_servers='localhost:9092',
        topic='trending-hashtags'
    )
    producer.run_continuous()
