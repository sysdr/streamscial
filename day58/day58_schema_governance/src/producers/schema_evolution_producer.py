"""
Schema Evolution Producer - Demonstrates gradual schema migration
Simulates production rollout with multiple schema versions
"""

import json
import time
import random
from typing import Dict, Any
from confluent_kafka import Producer
from src.registry.schema_manager import registry_client
import fastavro
from io import BytesIO


class SchemaEvolutionProducer:
    """
    Producer supporting multiple schema versions
    Simulates gradual migration: v1 -> v2 -> v3
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'schema-evolution-producer'
        })
        
        self.schemas = {}
        self.version_weights = {
            'v1': 0.50,  # 50% still on v1
            'v2': 0.35,  # 35% migrated to v2
            'v3': 0.15   # 15% early adopters on v3
        }
        
        self.metrics = {
            'sent_by_version': {'v1': 0, 'v2': 0, 'v3': 0},
            'total_sent': 0,
            'errors': 0
        }
    
    def load_schemas(self, schema_dir: str):
        """Load and register all schema versions"""
        print("\n=== Loading and Registering Schemas ===")
        
        schema_files = {
            'v1': f'{schema_dir}/posts/post_v1.avsc',
            'v2': f'{schema_dir}/posts/post_v2.avsc',
            'v3': f'{schema_dir}/posts/post_v3.avsc'
        }
        
        for version, filepath in schema_files.items():
            with open(filepath, 'r') as f:
                schema_str = f.read()
                schema_obj = json.loads(schema_str)
                
                # Register with registry
                schema_id, is_compatible = registry_client.register_schema(
                    subject='posts-value',
                    schema_str=schema_str,
                    compatibility_level='BACKWARD'
                )
                
                if is_compatible:
                    self.schemas[version] = {
                        'id': schema_id,
                        'schema_str': schema_str,
                        'schema_obj': schema_obj
                    }
                    print(f"✓ Loaded schema {version}: {schema_obj['doc']}")
                else:
                    print(f"✗ Schema {version} failed compatibility check")
    
    def create_post_v1(self, post_id: int, user_id: int) -> Dict[str, Any]:
        """Create v1 post (basic)"""
        return {
            'post_id': f'post_{post_id}',
            'user_id': f'user_{user_id}',
            'content': f'This is a basic post #{post_id} without polls or media',
            'timestamp': int(time.time() * 1000)
        }
    
    def create_post_v2(self, post_id: int, user_id: int) -> Dict[str, Any]:
        """Create v2 post (with polls)"""
        base = self.create_post_v1(post_id, user_id)
        
        # 50% chance of including poll
        if random.random() < 0.5:
            base['poll_data'] = {
                'question': f'What do you think about topic {post_id}?',
                'options': ['Strongly Agree', 'Agree', 'Neutral', 'Disagree'],
                'duration_hours': random.choice([24, 48, 72])
            }
        else:
            base['poll_data'] = None
        
        return base
    
    def create_post_v3(self, post_id: int, user_id: int) -> Dict[str, Any]:
        """Create v3 post (with rich media)"""
        base = self.create_post_v2(post_id, user_id)
        
        # 60% chance of including media
        if random.random() < 0.6:
            media_type = random.choice(['IMAGE', 'VIDEO', 'GIF', 'GALLERY'])
            num_items = 1 if media_type != 'GALLERY' else random.randint(2, 5)
            
            base['media'] = {
                'media_type': media_type,
                'urls': [f'https://cdn.streamsocial.com/media/{post_id}_{i}.jpg' for i in range(num_items)],
                'thumbnails': [f'https://cdn.streamsocial.com/thumb/{post_id}_{i}.jpg' for i in range(num_items)]
            }
        else:
            base['media'] = None
        
        return base
    
    def produce_with_version(self, version: str, post_data: Dict[str, Any]):
        """Produce message with specific schema version"""
        try:
            schema_info = self.schemas[version]
            
            # Serialize with Avro
            bytes_io = BytesIO()
            fastavro.schemaless_writer(bytes_io, schema_info['schema_obj'], post_data)
            avro_bytes = bytes_io.getvalue()
            
            # Prepend schema ID (4 bytes) - Schema Registry wire format
            schema_id_bytes = schema_info['id'].to_bytes(4, byteorder='big')
            message = schema_id_bytes + avro_bytes
            
            # Send to Kafka (simulated)
            print(f"→ Sent {version} post {post_data['post_id']} (schema_id={schema_info['id']}, size={len(message)}B)")
            
            self.metrics['sent_by_version'][version] += 1
            self.metrics['total_sent'] += 1
            
        except Exception as e:
            print(f"✗ Error producing {version}: {e}")
            self.metrics['errors'] += 1
    
    def simulate_gradual_migration(self, num_posts: int = 50):
        """
        Simulate gradual schema migration in production
        Weighted random selection mimics real-world rollout
        """
        print(f"\n=== Simulating Gradual Migration ({num_posts} posts) ===")
        print(f"Version distribution: v1={self.version_weights['v1']*100}%, "
              f"v2={self.version_weights['v2']*100}%, v3={self.version_weights['v3']*100}%\n")
        
        for i in range(num_posts):
            # Weighted random version selection
            rand = random.random()
            if rand < self.version_weights['v1']:
                version = 'v1'
                post_data = self.create_post_v1(i, random.randint(1, 100))
            elif rand < self.version_weights['v1'] + self.version_weights['v2']:
                version = 'v2'
                post_data = self.create_post_v2(i, random.randint(1, 100))
            else:
                version = 'v3'
                post_data = self.create_post_v3(i, random.randint(1, 100))
            
            self.produce_with_version(version, post_data)
            time.sleep(0.05)  # Simulate production rate
        
        print(f"\n=== Migration Complete ===")
        print(f"Total sent: {self.metrics['total_sent']}")
        print(f"By version: v1={self.metrics['sent_by_version']['v1']}, "
              f"v2={self.metrics['sent_by_version']['v2']}, "
              f"v3={self.metrics['sent_by_version']['v3']}")
        print(f"Errors: {self.metrics['errors']}")
    
    def get_metrics(self) -> dict:
        """Get producer metrics"""
        return self.metrics


# Create producer instance
evolution_producer = SchemaEvolutionProducer()
