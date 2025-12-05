import json
import time
import random
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import sys
import os
# Add src directory to path
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
from models import UserAction

class ActionStreamProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            compression_type='lz4'
        )
        self.topic = 'user-actions'
        self._create_topic()
        
    def _create_topic(self):
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        topic = NewTopic(
            name=self.topic,
            num_partitions=6,
            replication_factor=1
        )
        try:
            admin.create_topics([topic])
            print(f"Created topic: {self.topic}")
        except TopicAlreadyExistsError:
            print(f"Topic {self.topic} already exists")
        finally:
            admin.close()
    
    def simulate_actions(self, duration_seconds=300, actions_per_sec=100):
        """Generate user action stream"""
        action_types = ['post_liked', 'comment_created', 'post_shared', 
                       'user_followed', 'post_viewed', 'story_viewed']
        
        print(f"Simulating user actions for {duration_seconds}s at {actions_per_sec}/sec...")
        
        start_time = time.time()
        action_count = 0
        
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()
            
            for _ in range(actions_per_sec):
                user_id = f"user_{random.randint(0, 999)}"
                action = UserAction(
                    user_id=user_id,
                    action_type=random.choice(action_types),
                    target_id=f"target_{random.randint(1000, 9999)}",
                    timestamp=int(time.time() * 1000)
                )
                
                self.producer.send(
                    self.topic,
                    key=user_id,
                    value=action.to_dict()
                )
                action_count += 1
            
            # Maintain rate
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            
            if action_count % 1000 == 0:
                print(f"  Produced {action_count} actions...")
        
        self.producer.flush()
        print(f"Action stream complete: {action_count} actions")
    
    def close(self):
        self.producer.close()

if __name__ == '__main__':
    producer = ActionStreamProducer()
    producer.simulate_actions(duration_seconds=180, actions_per_sec=100)
    producer.close()
