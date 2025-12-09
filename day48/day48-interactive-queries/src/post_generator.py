import json
import time
import random
from kafka import KafkaProducer

# Trending hashtags with weights
HASHTAGS = {
    'AI': 50, 'MachineLearning': 45, 'DataScience': 40,
    'Python': 35, 'Cloud': 30, 'DevOps': 25,
    'Kubernetes': 20, 'Docker': 20, 'AWS': 18,
    'Tech': 15, 'Innovation': 12, 'Startup': 10,
    'BigData': 8, 'IoT': 7, 'Blockchain': 6,
    'Cybersecurity': 5, 'WebDev': 5, 'Mobile': 4
}

def generate_post():
    """Generate realistic post with hashtags"""
    num_hashtags = random.randint(1, 4)
    hashtags = random.choices(
        list(HASHTAGS.keys()),
        weights=list(HASHTAGS.values()),
        k=num_hashtags
    )
    
    text = f"Exciting developments in {' '.join(['#' + h for h in hashtags])}!"
    
    return {
        'post_id': f"post_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        'user_id': f"user_{random.randint(1, 1000)}",
        'text': text,
        'timestamp': int(time.time() * 1000)
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    
    print("Starting post generator...")
    print("Generating posts with trending hashtags...")
    
    try:
        count = 0
        while True:
            post = generate_post()
            producer.send('social.posts', post)
            count += 1
            
            if count % 10 == 0:
                print(f"Generated {count} posts...")
            
            # Vary rate: burst then pause
            if random.random() < 0.3:
                time.sleep(0.1)  # Fast
            else:
                time.sleep(1)  # Slow
                
    except KeyboardInterrupt:
        print(f"\nGenerated {count} total posts")
        producer.close()

if __name__ == '__main__':
    main()
