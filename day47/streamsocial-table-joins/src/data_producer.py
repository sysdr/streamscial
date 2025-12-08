"""
Data Producer for User Preferences and Content Metadata Tables
Simulates updates to both KTables
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TableDataProducer:
    """Produces updates to both user preferences and content metadata tables"""
    
    LANGUAGES = ['english', 'spanish', 'french', 'german', 'japanese', 'korean']
    CATEGORIES = ['comedy', 'drama', 'documentary', 'news', 'sports', 'music', 'tech', 'gaming']
    TOPICS = ['politics', 'science', 'entertainment', 'health', 'travel', 'food', 'fashion', 'finance']
    
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
    
    def generate_user_preference(self, user_id: str) -> dict:
        """Generate realistic user preference data"""
        num_languages = random.randint(1, 3)
        num_categories = random.randint(2, 4)
        num_topics = random.randint(3, 6)
        
        return {
            'key': user_id,
            'value': {
                'languages': random.sample(self.LANGUAGES, num_languages),
                'categories': random.sample(self.CATEGORIES, num_categories),
                'topics': random.sample(self.TOPICS, num_topics),
                'weights': {
                    'language': round(random.uniform(0.7, 1.0), 2),
                    'category': round(random.uniform(0.7, 1.0), 2),
                    'topics': round(random.uniform(0.6, 1.0), 2)
                },
                'updated_at': datetime.now().isoformat()
            }
        }
    
    def generate_content_metadata(self, content_id: str) -> dict:
        """Generate realistic content metadata"""
        category = random.choice(self.CATEGORIES)
        
        return {
            'key': content_id,
            'value': {
                'language': random.choice(self.LANGUAGES),
                'category': category,
                'tags': random.sample(self.TOPICS, random.randint(2, 5)),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
        }
    
    def produce_initial_data(self, num_users: int = 20, num_content: int = 50):
        """Produce initial dataset to populate both tables"""
        logger.info(f"Producing initial data: {num_users} users, {num_content} content items")
        
        # Produce user preferences
        for i in range(num_users):
            user_id = f"user_{i:03d}"
            pref_data = self.generate_user_preference(user_id)
            self.producer.send('user-preferences', key=pref_data['key'], value=pref_data)
            time.sleep(0.1)
        
        logger.info(f"Produced {num_users} user preferences")
        
        # Produce content metadata
        for i in range(num_content):
            content_id = f"content_{i:04d}"
            content_data = self.generate_content_metadata(content_id)
            self.producer.send('content-metadata', key=content_data['key'], value=content_data)
            time.sleep(0.1)
        
        logger.info(f"Produced {num_content} content items")
        self.producer.flush()
    
    def produce_updates(self, duration_seconds: int = 300):
        """Continuously produce updates to both tables"""
        logger.info(f"Producing continuous updates for {duration_seconds} seconds")
        
        start_time = time.time()
        update_count = 0
        
        while time.time() - start_time < duration_seconds:
            # Randomly update either user preferences or content metadata
            if random.random() < 0.5:
                # Update user preference
                user_id = f"user_{random.randint(0, 19):03d}"
                pref_data = self.generate_user_preference(user_id)
                self.producer.send('user-preferences', key=pref_data['key'], value=pref_data)
                logger.info(f"Updated user preference: {user_id}")
            else:
                # Update content metadata
                content_id = f"content_{random.randint(0, 49):04d}"
                content_data = self.generate_content_metadata(content_id)
                self.producer.send('content-metadata', key=content_data['key'], value=content_data)
                logger.info(f"Updated content metadata: {content_id}")
            
            update_count += 1
            time.sleep(random.uniform(1, 3))
        
        self.producer.flush()
        logger.info(f"Produced {update_count} updates")
    
    def close(self):
        self.producer.close()


if __name__ == "__main__":
    producer = TableDataProducer('localhost:9092')
    
    # Produce initial data
    producer.produce_initial_data(num_users=20, num_content=50)
    
    # Produce continuous updates
    producer.produce_updates(duration_seconds=300)
    
    producer.close()
