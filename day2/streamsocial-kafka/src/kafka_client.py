"""
StreamSocial Kafka Client - Day 2
Handles broker connections, failover, and cluster monitoring
"""

import json
import time
import random
from kafka import KafkaProducer, KafkaConsumer, KafkaClient
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamSocialKafkaClient:
    def __init__(self, bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094']):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.admin_client = None
        self._init_clients()

    def _init_clients(self):
        """Initialize Kafka producer and admin client with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8'),
                    retries=3,
                    acks='all',  # Wait for all replicas
                    compression_type='gzip'
                )
                
                self.admin_client = KafkaAdminClient(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id='streamsocial-admin'
                )
                
                logger.info("✅ Kafka clients initialized successfully")
                return
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise

    def send_event(self, topic, event_data, user_id=None):
        """Send event to Kafka with automatic partitioning"""
        try:
            key = user_id if user_id else str(random.randint(1, 1000))
            
            future = self.producer.send(
                topic=topic,
                key=key,
                value=event_data,
                timestamp_ms=int(time.time() * 1000)
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.info(f"📤 Event sent to {topic}[{record_metadata.partition}] offset {record_metadata.offset}")
            return record_metadata
            
        except KafkaError as e:
            logger.error(f"❌ Failed to send event: {e}")
            raise

    def get_cluster_metadata(self):
        """Get cluster information and broker health"""
        try:
            client = KafkaClient(bootstrap_servers=self.bootstrap_servers)
            metadata = client.cluster
            
            brokers = []
            for broker in metadata.brokers():
                brokers.append({
                    'id': broker.nodeId,
                    'host': broker.host,
                    'port': broker.port,
                    'rack': broker.rack
                })
            
            topics = list(metadata.topics())
            
            return {
                'brokers': brokers,
                'topics': topics,
                'cluster_id': metadata.cluster_id,
                'controller': metadata.controller.nodeId if metadata.controller else None
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get cluster metadata: {e}")
            return None

    def create_streamsocial_topics(self):
        """Create StreamSocial topics with proper replication"""
        from kafka.admin import NewTopic
        
        topics = [
            NewTopic(
                name='user-actions',
                num_partitions=3,
                replication_factor=3
            ),
            NewTopic(
                name='content-interactions', 
                num_partitions=3,
                replication_factor=3
            ),
            NewTopic(
                name='system-events',
                num_partitions=1,
                replication_factor=3
            )
        ]
        
        try:
            result = self.admin_client.create_topics(topics, timeout_ms=10000)
            
            for topic_name, future in result.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic '{topic_name}' created successfully")
                except Exception as e:
                    if "TopicExistsException" in str(e):
                        logger.info(f"ℹ️ Topic '{topic_name}' already exists")
                    else:
                        logger.error(f"❌ Failed to create topic '{topic_name}': {e}")
                        
        except Exception as e:
            logger.error(f"❌ Failed to create topics: {e}")

    def simulate_user_activity(self, duration_seconds=60):
        """Simulate StreamSocial user activity for testing"""
        logger.info(f"🎭 Starting user activity simulation for {duration_seconds} seconds")
        
        user_actions = ['post', 'like', 'comment', 'share', 'follow']
        content_types = ['photo', 'video', 'text', 'story']
        
        start_time = time.time()
        event_count = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                # Generate user action event
                user_id = random.randint(1, 10000)
                action = random.choice(user_actions)
                
                user_event = {
                    'event_type': 'user_action',
                    'user_id': user_id,
                    'action': action,
                    'content_type': random.choice(content_types),
                    'timestamp': int(time.time() * 1000),
                    'session_id': f"session_{user_id}_{int(time.time())}"
                }
                
                self.send_event('user-actions', user_event, user_id)
                event_count += 1
                
                # Generate content interaction event
                if random.random() < 0.7:  # 70% chance
                    interaction_event = {
                        'event_type': 'content_interaction',
                        'user_id': user_id,
                        'content_id': random.randint(1, 1000),
                        'interaction_type': random.choice(['view', 'click', 'scroll']),
                        'duration_ms': random.randint(1000, 30000),
                        'timestamp': int(time.time() * 1000)
                    }
                    
                    self.send_event('content-interactions', interaction_event, user_id)
                    event_count += 1
                
                time.sleep(random.uniform(0.1, 0.5))  # Random delay
                
        except KeyboardInterrupt:
            logger.info("🛑 Simulation stopped by user")
        
        logger.info(f"📊 Simulation complete: {event_count} events generated")
        return event_count

if __name__ == "__main__":
    # Demo the client
    client = StreamSocialKafkaClient()
    
    # Create topics
    client.create_streamsocial_topics()
    
    # Get cluster info
    metadata = client.get_cluster_metadata()
    if metadata:
        print(f"🏢 Cluster ID: {metadata['cluster_id']}")
        print(f"👑 Controller: Broker {metadata['controller']}")
        print(f"🖥️ Brokers: {len(metadata['brokers'])}")
        for broker in metadata['brokers']:
            print(f"  - Broker {broker['id']}: {broker['host']}:{broker['port']}")
    
    # Run simulation
    client.simulate_user_activity(30)
