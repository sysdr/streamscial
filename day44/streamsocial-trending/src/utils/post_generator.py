from kafka import KafkaProducer
from datetime import datetime
import json
import random
import time
import uuid

class PostGenerator:
    """Generates synthetic posts for testing"""
    
    TRENDING_HASHTAGS = [
        "ai", "ml", "python", "javascript", "react", "kubernetes",
        "blockchain", "crypto", "nft", "metaverse", "gaming", "esports",
        "climate", "sustainability", "renewable", "electric", "tech",
        "startup", "venture", "innovation", "disruption"
    ]
    
    REGIONS = ["us-west", "us-east", "eu-west", "eu-central", "asia-pacific", "global"]
    LANGUAGES = ["en", "es", "fr", "de", "ja", "zh"]
    
    POST_TEMPLATES = [
        "Just discovered #{} and I'm blown away! #{} #{}",
        "Hot take: #{} is the future of {}. #{} #tech",
        "Can't believe {} is trending! #{} #{} #viral",
        "New research on #{} shows amazing results. #{} #science",
        "Breaking: Major development in #{}. This changes everything! #{}",
        "Learning #{} and loving it! Any tips? #{} #coding",
        "The {} revolution is here. #{} #{} #future"
    ]
    
    def __init__(self, bootstrap_servers: str, topic: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.post_count = 0
    
    def generate_post(self) -> dict:
        """Generate a random post with hashtags"""
        # Select 2-3 hashtags for this post
        num_hashtags = random.randint(2, 3)
        hashtags = random.sample(self.TRENDING_HASHTAGS, num_hashtags)
        
        # Create post content
        template = random.choice(self.POST_TEMPLATES)
        content = template.format(*hashtags, random.choice(["tech", "news", "trending"]))
        
        post = {
            'post_id': str(uuid.uuid4()),
            'user_id': f"user_{random.randint(1000, 9999)}",
            'content': content,
            'created_at': datetime.now().isoformat(),
            'region': random.choice(self.REGIONS),
            'language': random.choice(self.LANGUAGES),
            'engagement_score': random.uniform(0.1, 10.0)
        }
        
        return post
    
    def start_generating(self, posts_per_second: int = 10, duration_seconds: int = 300):
        """Generate posts at specified rate"""
        interval = 1.0 / posts_per_second
        end_time = time.time() + duration_seconds
        
        print(f"Generating {posts_per_second} posts/sec for {duration_seconds} seconds...")
        
        while time.time() < end_time:
            start = time.time()
            
            post = self.generate_post()
            self.producer.send(self.topic, value=post)
            self.post_count += 1
            
            if self.post_count % 100 == 0:
                print(f"Generated {self.post_count} posts...")
            
            # Sleep to maintain rate
            elapsed = time.time() - start
            if elapsed < interval:
                time.sleep(interval - elapsed)
        
        self.producer.flush()
        print(f"Completed! Generated {self.post_count} total posts.")

if __name__ == "__main__":
    generator = PostGenerator("localhost:9092", "streamsocial.posts")
    generator.start_generating(posts_per_second=20, duration_seconds=600)
