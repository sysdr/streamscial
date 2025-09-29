#!/usr/bin/env python3

import json
import time
import random
from kafka import KafkaProducer
from config.kafka_config import KafkaConfig

def generate_continuous_trends():
    """Generate continuous trending data for the dashboard"""
    producer = KafkaProducer(
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    hashtags = ['ai', 'python', 'kafka', 'bigdata', 'streaming', 'analytics', 'tech', 'innovation', 'startup', 'coding', 'developer', 'opensource', 'cloud', 'machinelearning', 'realtime']
    
    print("Generating continuous trending data...")
    
    try:
        while True:
            # Generate trends for each worker
            for worker_id in range(3):
                worker_name = f"trend-worker-{worker_id}"
                
                # Generate 2-4 random trends per worker
                num_trends = random.randint(2, 4)
                selected_hashtags = random.sample(hashtags, num_trends)
                
                trends = []
                for hashtag in selected_hashtags:
                    count = random.randint(10, 100)
                    velocity = random.uniform(2.0, 20.0)
                    unique_users = random.randint(5, 50)
                    trend_score = velocity * (1 + unique_users * 0.1)
                    
                    trends.append({
                        'hashtag': hashtag,
                        'count': count,
                        'velocity': round(velocity, 2),
                        'unique_users': unique_users,
                        'trend_score': round(trend_score, 2),
                        'worker_id': worker_id,
                        'timestamp': time.time()
                    })
                
                # Sort by trend score
                trends.sort(key=lambda x: x['trend_score'], reverse=True)
                
                trend_data = {
                    'worker_name': worker_name,
                    'trends': trends,
                    'timestamp': time.time()
                }
                
                producer.send(KafkaConfig.TRENDING_RESULTS_TOPIC, trend_data)
            
            producer.flush()
            print(f"Published trending data at {time.strftime('%H:%M:%S')}")
            time.sleep(5)  # Update every 5 seconds
            
    except KeyboardInterrupt:
        print("\nStopping trend generator...")
        producer.close()

if __name__ == "__main__":
    generate_continuous_trends()
