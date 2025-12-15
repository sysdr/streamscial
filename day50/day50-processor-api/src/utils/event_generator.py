"""
Event generator - simulates 50K events/sec
"""
import json
import time
import random
import threading
from kafka import KafkaProducer

class EventGenerator:
    """Generates realistic engagement events"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        
        self.topics = {
            'engagement': 'user-engagement',
            'content': 'content-metadata',
            'preferences': 'user-preferences'
        }
        
        self.running = False
        self.metrics = {
            'events_sent': 0,
            'rate_per_sec': 0
        }
        
        self.event_types = ['view', 'like', 'comment', 'share', 'bookmark']
        self.categories = ['tech', 'sports', 'entertainment', 'news', 'lifestyle']
        
    def start(self, target_rate: int = 1000):
        """Start generating events at target rate"""
        self.running = True
        self.target_rate = target_rate
        
        # Start generation threads
        threading.Thread(target=self._generate_engagement, daemon=True).start()
        threading.Thread(target=self._generate_content, daemon=True).start()
        threading.Thread(target=self._generate_preferences, daemon=True).start()
        
        print(f"Event generator started. Target rate: {target_rate} events/sec")
    
    def _generate_engagement(self):
        """Generate engagement events"""
        while self.running:
            try:
                user_id = f"user_{random.randint(1, 10000)}"
                content_id = f"content_{random.randint(1, 100000)}"
                
                event = {
                    'event_type': random.choice(self.event_types),
                    'user_id': user_id,
                    'content_id': content_id,
                    'timestamp': int(time.time()),
                    'dwell_time': random.randint(10, 300),
                    'interactions': random.randint(0, 20),
                    'viral_score': random.random()
                }
                
                self.producer.send(
                    self.topics['engagement'],
                    key=user_id,
                    value=event
                )
                
                self.metrics['events_sent'] += 1
                
                # Rate limiting
                time.sleep(1.0 / (self.target_rate * 0.7))  # 70% engagement events
                
            except Exception as e:
                print(f"Error generating engagement: {e}")
    
    def _generate_content(self):
        """Generate content metadata events"""
        while self.running:
            try:
                content_id = f"content_{random.randint(1, 100000)}"
                creator_id = f"creator_{random.randint(1, 1000)}"
                
                event = {
                    'event_type': 'content_publish',
                    'content_id': content_id,
                    'creator_id': creator_id,
                    'category': random.choice(self.categories),
                    'timestamp': int(time.time()),
                    'viral_velocity': random.random(),
                    'creator_followers': random.randint(100, 1000000)
                }
                
                self.producer.send(
                    self.topics['content'],
                    key=content_id,
                    value=event
                )
                
                self.metrics['events_sent'] += 1
                
                time.sleep(1.0 / (self.target_rate * 0.2))  # 20% content events
                
            except Exception as e:
                print(f"Error generating content: {e}")
    
    def _generate_preferences(self):
        """Generate user preference events"""
        while self.running:
            try:
                user_id = f"user_{random.randint(1, 10000)}"
                
                event = {
                    'event_type': 'preference_update',
                    'user_id': user_id,
                    'timestamp': int(time.time()),
                    'followed_categories': random.sample(self.categories, k=random.randint(1, 3)),
                    'followed_creators': [f"creator_{random.randint(1, 1000)}" for _ in range(random.randint(5, 20))]
                }
                
                self.producer.send(
                    self.topics['preferences'],
                    key=user_id,
                    value=event
                )
                
                self.metrics['events_sent'] += 1
                
                time.sleep(1.0 / (self.target_rate * 0.1))  # 10% preference events
                
            except Exception as e:
                print(f"Error generating preferences: {e}")
    
    def stop(self):
        """Stop generator"""
        self.running = False
        self.producer.flush()
        self.producer.close()
        print(f"Event generator stopped. Total events: {self.metrics['events_sent']}")
    
    def get_metrics(self):
        """Get generation metrics"""
        return self.metrics.copy()
