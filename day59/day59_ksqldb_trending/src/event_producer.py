"""Event producer for StreamSocial posts, likes, comments, views"""
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class StreamSocialEventProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.hashtags = ['tech', 'startup', 'ai', 'coding', 'design', 'product', 
                         'data', 'cloud', 'security', 'devops', 'python', 'react',
                         'kubernetes', 'ml', 'blockchain', 'web3', 'saas', 'mobile']
        self.user_ids = list(range(1, 1001))
        self.post_id_counter = 0
        
    def generate_post(self):
        """Generate realistic post event"""
        self.post_id_counter += 1
        num_hashtags = random.randint(0, 4)
        selected_tags = random.sample(self.hashtags, num_hashtags) if num_hashtags > 0 else []
        
        content = fake.text(max_nb_chars=200)
        if selected_tags:
            content += " " + " ".join([f"#{tag}" for tag in selected_tags])
        
        return {
            'post_id': self.post_id_counter,
            'user_id': random.choice(self.user_ids),
            'content': content,
            'hashtags': selected_tags,
            'created_at': datetime.now().isoformat(),
            'media_type': random.choice(['text', 'image', 'video', 'link']),
            'timestamp': int(time.time() * 1000)
        }
    
    def generate_like(self, post_id):
        """Generate like event for a post"""
        return {
            'like_id': int(time.time() * 1000000),
            'post_id': post_id,
            'user_id': random.choice(self.user_ids),
            'created_at': datetime.now().isoformat(),
            'timestamp': int(time.time() * 1000)
        }
    
    def generate_comment(self, post_id):
        """Generate comment event for a post"""
        return {
            'comment_id': int(time.time() * 1000000),
            'post_id': post_id,
            'user_id': random.choice(self.user_ids),
            'content': fake.sentence(),
            'created_at': datetime.now().isoformat(),
            'timestamp': int(time.time() * 1000)
        }
    
    def generate_view(self, post_id):
        """Generate view event for a post"""
        return {
            'view_id': int(time.time() * 1000000),
            'post_id': post_id,
            'user_id': random.choice(self.user_ids),
            'duration_ms': random.randint(100, 30000),
            'created_at': datetime.now().isoformat(),
            'timestamp': int(time.time() * 1000)
        }
    
    def send_event(self, topic, event):
        """Send event to Kafka topic"""
        try:
            future = self.producer.send(topic, event)
            future.get(timeout=10)
        except Exception as e:
            logger.error(f"Failed to send event to {topic}: {e}")
    
    def generate_traffic(self, duration_seconds=300, posts_per_sec=50):
        """Generate realistic traffic pattern"""
        logger.info(f"Starting event generation for {duration_seconds} seconds...")
        logger.info(f"Target: {posts_per_sec} posts/sec, ~{posts_per_sec*4} likes/sec, ~{posts_per_sec*0.6} comments/sec")
        
        start_time = time.time()
        posts_generated = 0
        active_posts = []
        
        while time.time() - start_time < duration_seconds:
            cycle_start = time.time()
            
            # Generate posts
            for _ in range(posts_per_sec):
                post = self.generate_post()
                self.send_event('posts', post)
                active_posts.append(post['post_id'])
                posts_generated += 1
                
                # Keep only recent posts for engagement
                if len(active_posts) > 1000:
                    active_posts = active_posts[-1000:]
            
            # Generate likes (4x posts)
            for _ in range(posts_per_sec * 4):
                if active_posts:
                    post_id = random.choice(active_posts[-100:])  # Focus on recent posts
                    like = self.generate_like(post_id)
                    self.send_event('likes', like)
            
            # Generate comments (0.6x posts)
            for _ in range(int(posts_per_sec * 0.6)):
                if active_posts:
                    post_id = random.choice(active_posts[-100:])
                    comment = self.generate_comment(post_id)
                    self.send_event('comments', comment)
            
            # Generate views (20x posts)
            for _ in range(posts_per_sec * 20):
                if active_posts:
                    post_id = random.choice(active_posts[-200:])
                    view = self.generate_view(post_id)
                    self.send_event('views', view)
            
            # Maintain rate
            elapsed = time.time() - cycle_start
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)
            
            if posts_generated % 500 == 0:
                logger.info(f"Generated {posts_generated} posts, {len(active_posts)} active posts")
        
        self.producer.flush()
        logger.info(f"Event generation complete. Total posts: {posts_generated}")
    
    def close(self):
        """Close producer"""
        self.producer.close()

if __name__ == '__main__':
    producer = StreamSocialEventProducer()
    try:
        producer.generate_traffic(duration_seconds=300, posts_per_sec=50)
    finally:
        producer.close()
