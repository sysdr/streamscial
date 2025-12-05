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
from models import UserProfile

class ProfileChangelogProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            compression_type='lz4'
        )
        self.topic = 'user-profiles'
        self._create_topic()
        
    def _create_topic(self):
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        topic = NewTopic(
            name=self.topic,
            num_partitions=6,
            replication_factor=1,
            topic_configs={'cleanup.policy': 'compact'}
        )
        try:
            admin.create_topics([topic])
            print(f"Created compacted topic: {self.topic}")
        except TopicAlreadyExistsError:
            print(f"Topic {self.topic} already exists")
        finally:
            admin.close()
    
    def generate_initial_profiles(self, num_users=1000):
        """Generate initial user profile state"""
        cities = ['SF', 'NYC', 'LA', 'Seattle', 'Austin', 'Boston', 'Chicago', 'Denver']
        interest_pool = ['tech', 'gaming', 'sports', 'music', 'travel', 'food', 'art', 'fitness']
        tiers = ['free', 'basic', 'premium', 'enterprise']
        
        print(f"Generating {num_users} initial user profiles...")
        for i in range(num_users):
            user_id = f"user_{i}"
            profile = UserProfile(
                user_id=user_id,
                age=random.randint(18, 65),
                city=random.choice(cities),
                interests=random.sample(interest_pool, k=random.randint(2, 5)),
                tier=random.choice(tiers),
                joined_date=f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            )
            
            self.producer.send(
                self.topic,
                key=user_id,
                value=profile.to_dict()
            )
            
            if (i + 1) % 100 == 0:
                print(f"  Produced {i + 1} profiles...")
        
        self.producer.flush()
        print(f"Initial profile load complete: {num_users} profiles")
    
    def simulate_profile_updates(self, duration_seconds=300, updates_per_sec=5):
        """Simulate ongoing profile updates"""
        print(f"Simulating profile updates for {duration_seconds}s at {updates_per_sec}/sec...")
        
        cities = ['SF', 'NYC', 'LA', 'Seattle', 'Austin', 'Boston', 'Chicago', 'Denver']
        tiers = ['free', 'basic', 'premium', 'enterprise']
        
        start_time = time.time()
        update_count = 0
        
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()
            
            for _ in range(updates_per_sec):
                user_id = f"user_{random.randint(0, 999)}"
                
                # Simulate different update types
                update_type = random.choice(['city', 'tier', 'age'])
                
                if update_type == 'city':
                    profile = {
                        'user_id': user_id,
                        'city': random.choice(cities),
                        'update_type': 'location_change'
                    }
                elif update_type == 'tier':
                    profile = {
                        'user_id': user_id,
                        'tier': random.choice(tiers),
                        'update_type': 'subscription_change'
                    }
                else:
                    profile = {
                        'user_id': user_id,
                        'age': random.randint(18, 65),
                        'update_type': 'profile_update'
                    }
                
                self.producer.send(self.topic, key=user_id, value=profile)
                update_count += 1
            
            # Sleep to maintain rate
            elapsed = time.time() - batch_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
        
        self.producer.flush()
        print(f"Profile updates complete: {update_count} updates")
    
    def close(self):
        self.producer.close()

if __name__ == '__main__':
    producer = ProfileChangelogProducer()
    
    # Generate initial state
    producer.generate_initial_profiles(1000)
    
    # Simulate ongoing updates
    time.sleep(2)  # Let KTable materialize
    producer.simulate_profile_updates(duration_seconds=180, updates_per_sec=5)
    
    producer.close()
