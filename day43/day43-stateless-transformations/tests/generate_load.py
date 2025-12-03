"""Load generator for testing the pipeline."""

import json
import time
import random
import argparse
from confluent_kafka import Producer
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_RAW_POSTS

# Sample content templates
CLEAN_POSTS = [
    "Just finished reading an amazing book on {topic}! Highly recommend it.",
    "Beautiful sunset today at the beach. #nature #photography",
    "Excited to announce my new project launching next week! #startup",
    "Great article about {topic}. Really insightful perspective.",
    "Coffee and coding - perfect morning! #developer #programming",
    "@alice check out this cool {topic} resource I found!",
    "Weekend plans: hiking and relaxation. What about you?",
    "The {topic} conference was incredible. So many great talks!",
]

SPAM_POSTS = [
    "BUY NOW!!! Limited time offer - click here!!!",
    "🔥🔥🔥 GET RICH QUICK 🔥🔥🔥 Call now!!!",
    "FREE MONEY!!! No risk, 100% guaranteed!!!",
    "CLICK HERE NOW!!! Act fast before it's too late!!!",
]

TOPICS = ['AI', 'blockchain', 'cloud computing', 'machine learning', 'web development']
USERS = [f'user{i}' for i in range(1000)]

def generate_post():
    """Generate a random post."""
    # 85% clean, 15% spam
    if random.random() < 0.85:
        template = random.choice(CLEAN_POSTS)
        content = template.format(topic=random.choice(TOPICS))
    else:
        content = random.choice(SPAM_POSTS)
    
    return {
        'id': f'post{int(time.time() * 1000000)}',
        'user_id': random.choice(USERS),
        'content': content,
        'has_image': random.random() < 0.3,
        'timestamp': time.time()
    }

def main():
    parser = argparse.ArgumentParser(description='Generate load for testing')
    parser.add_argument('--rate', type=int, default=100, help='Posts per second')
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds')
    args = parser.parse_args()
    
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    
    print(f"[INFO] Generating {args.rate} posts/second for {args.duration} seconds")
    print(f"[INFO] Total posts: {args.rate * args.duration}")
    
    start_time = time.time()
    total_produced = 0
    
    while time.time() - start_time < args.duration:
        batch_start = time.time()
        
        # Produce batch
        for _ in range(args.rate):
            post = generate_post()
            producer.produce(
                TOPIC_RAW_POSTS,
                key=post['id'].encode('utf-8'),
                value=json.dumps(post).encode('utf-8')
            )
            total_produced += 1
        
        producer.flush()
        
        # Sleep to maintain rate
        elapsed = time.time() - batch_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        
        if total_produced % 1000 == 0:
            print(f"[INFO] Produced {total_produced} posts...")
    
    print(f"[INFO] Load generation complete. Total: {total_produced} posts")

if __name__ == '__main__':
    main()
