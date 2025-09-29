import json
import time
import random
import threading
from kafka import KafkaProducer
from config.kafka_config import KafkaConfig
import uuid

class SocialPostProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        
        self.running = False
        self.thread = None
        
        # Sample data for realistic posts
        self.sample_hashtags = [
            'ai', 'machinelearning', 'python', 'kafka', 'bigdata',
            'streaming', 'realtime', 'analytics', 'tech', 'innovation',
            'startup', 'coding', 'developer', 'opensource', 'cloud'
        ]
        
        self.sample_texts = [
            "Just deployed our new {hashtag} system!",
            "Amazing insights from {hashtag} conference today",
            "Working on {hashtag} project - love the possibilities",
            "New breakthrough in {hashtag} research published",
            "Great {hashtag} tutorial I found online",
            "Building the future with {hashtag} technology"
        ]
    
    def start_producing(self, posts_per_second: int = 5):
        """Start producing social media posts"""
        self.running = True
        self.thread = threading.Thread(
            target=self._produce_loop,
            args=(posts_per_second,)
        )
        self.thread.start()
    
    def stop_producing(self):
        """Stop producing posts"""
        self.running = False
        if self.thread:
            self.thread.join()
        self.producer.close()
    
    def _produce_loop(self, posts_per_second: int):
        """Main production loop"""
        interval = 1.0 / posts_per_second
        
        while self.running:
            post = self._generate_post()
            
            # Use hashtag as key for consistent partitioning
            hashtags = [tag for tag in post['text'] if tag.startswith('#')]
            partition_key = hashtags[0] if hashtags else None
            
            self.producer.send(
                KafkaConfig.SOCIAL_POSTS_TOPIC,
                value=post,
                key=partition_key
            )
            
            time.sleep(interval)
    
    def _generate_post(self) -> dict:
        """Generate a realistic social media post"""
        # Select random hashtags
        num_hashtags = random.randint(1, 3)
        selected_hashtags = random.sample(self.sample_hashtags, num_hashtags)
        
        # Create post text
        base_text = random.choice(self.sample_texts)
        hashtag_text = random.choice(selected_hashtags)
        
        post_text = base_text.format(hashtag=hashtag_text)
        
        # Add hashtags
        for hashtag in selected_hashtags:
            post_text += f" #{hashtag}"
        
        return {
            'post_id': str(uuid.uuid4()),
            'user_id': f"user_{random.randint(1, 1000)}",
            'text': post_text,
            'timestamp': time.time(),
            'likes': random.randint(0, 100),
            'retweets': random.randint(0, 50)
        }
