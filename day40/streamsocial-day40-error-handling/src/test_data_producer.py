"""Test data producer for user posts"""
import json
import time
import random
from confluent_kafka import Producer

def create_test_posts():
    """Generate test posts with various content types"""
    posts = [
        {"userId": "user-001", "content": "Just finished an amazing workout! 💪"},
        {"userId": "user-002", "content": "Check out my new blog post about Python"},
        {"userId": "user-003", "content": "This is spam content - buy now!"},
        {"userId": "user-004", "content": "Beautiful sunset today 🌅"},
        {"userId": "user-005", "content": "Hate and violence should never be tolerated"},
        {"userId": "user-006", "content": "Great movie recommendation for the weekend"},
        {"userId": "user-007", "content": "Scam alert! Don't fall for this"},
        {"userId": "user-008", "content": "Learning Kafka has been an incredible journey"},
        {"userId": "user-009", "content": "🔥💀 Invalid UTF-8 test: \x84\x85"},  # Will cause encoding error
        {"userId": "user-010", "content": "Normal post with no issues"},
    ]
    return posts

def produce_posts(bootstrap_servers='localhost:9092', topic='user-posts', count=100):
    """Produce test posts to Kafka"""
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    posts = create_test_posts()
    
    print(f"Producing {count} test posts to '{topic}'...\n")
    
    for i in range(count):
        post = random.choice(posts)
        post_with_id = {
            **post,
            "postId": f"post-{i+1}",
            "timestamp": int(time.time() * 1000)
        }
        
        key = post_with_id["postId"]
        value = json.dumps(post_with_id)
        
        try:
            producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=value.encode('utf-8')
            )
            
            if (i + 1) % 10 == 0:
                print(f"Produced {i + 1} posts...")
                producer.flush()
        except Exception as e:
            print(f"Error producing post {i+1}: {e}")
    
    producer.flush()
    print(f"\n✓ Successfully produced {count} posts to '{topic}'")

if __name__ == '__main__':
    produce_posts(count=100)
