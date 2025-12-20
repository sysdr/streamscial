"""SASL-authenticated Kafka producer for StreamSocial posts."""

import json
import time
from typing import Dict, Optional
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker


class SALSProducer:
    """Kafka producer with SASL authentication."""
    
    def __init__(self, bootstrap_servers: str, username: str, password: str, 
                 mechanism: str = 'SCRAM-SHA-512'):
        self.bootstrap_servers = bootstrap_servers
        self.username = username
        self.mechanism = mechanism
        self.faker = Faker()
        
        sasl_config = {
            'bootstrap_servers': bootstrap_servers,
            'security_protocol': 'SASL_PLAINTEXT',
            'sasl_mechanism': mechanism,
            'sasl_plain_username': username,
            'sasl_plain_password': password,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda v: v.encode('utf-8') if v else None,
            'acks': 'all',
            'retries': 3
        }
        
        self.producer = KafkaProducer(**sasl_config)
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'auth_failures': 0,
            'last_send_time': None
        }
    
    def produce_post(self, topic: str, user_id: str, content: str) -> bool:
        """Produce a post event to Kafka."""
        try:
            post_event = {
                'event_type': 'post_created',
                'post_id': f"post_{int(time.time() * 1000000)}",
                'user_id': user_id,
                'content': content,
                'timestamp': datetime.now().isoformat(),
                'producer_auth': self.mechanism
            }
            
            future = self.producer.send(topic, key=user_id, value=post_event)
            result = future.get(timeout=10)
            
            self.stats['messages_sent'] += 1
            self.stats['last_send_time'] = datetime.now().isoformat()
            return True
            
        except KafkaError as e:
            self.stats['messages_failed'] += 1
            if 'authentication' in str(e).lower():
                self.stats['auth_failures'] += 1
            print(f"❌ Failed to send message: {e}")
            return False
    
    def produce_batch(self, topic: str, count: int = 100):
        """Produce a batch of random posts."""
        print(f"📤 Producing {count} posts with {self.mechanism} auth...")
        
        for i in range(count):
            user_id = f"user_{self.faker.random_int(min=1, max=1000)}"
            content = self.faker.sentence(nb_words=10)
            self.produce_post(topic, user_id, content)
            
            if (i + 1) % 20 == 0:
                print(f"   Sent {i + 1}/{count} messages...")
        
        self.producer.flush()
        print(f"✅ Batch complete. Sent: {self.stats['messages_sent']}, Failed: {self.stats['messages_failed']}")
    
    def get_stats(self) -> Dict:
        """Get producer statistics."""
        return self.stats.copy()
    
    def close(self):
        """Close producer connection."""
        self.producer.close()
