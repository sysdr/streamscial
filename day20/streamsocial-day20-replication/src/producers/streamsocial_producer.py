"""
StreamSocial Multi-Region Producer with Replication Awareness
"""
import json
import time
import random
import logging
from typing import Dict, Optional, List
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamSocialProducer:
    def __init__(self, bootstrap_servers: str, region: str = "us-east"):
        self.bootstrap_servers = bootstrap_servers
        self.region = region
        
        # Producer configuration optimized for replication
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'streamsocial-producer-{region}',
            'acks': 'all',  # Wait for all in-sync replicas
            'retries': 10,
            'retry.backoff.ms': 100,
            'batch.size': 16384,
            'linger.ms': 5,
            'compression.type': 'lz4',
            'max.in.flight.requests.per.connection': 5,
            'enable.idempotence': True,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 120000
        }
        
        self.producer = Producer(producer_config)
        self.admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        
        # Message counters for monitoring
        self.messages_sent = 0
        self.messages_failed = 0
        self.delivery_callbacks = {}
        
    def create_topics(self):
        """Create topics with proper replication configuration"""
        with open('config/kafka/topics.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        topics_to_create = []
        for topic_name, topic_config in config['topics'].items():
            topic = NewTopic(
                topic_name,
                num_partitions=topic_config['partitions'],
                replication_factor=topic_config['replication_factor'],
                config=topic_config['config']
            )
            topics_to_create.append(topic)
        
        try:
            futures = self.admin_client.create_topics(topics_to_create)
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"✅ Topic {topic} created successfully")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.info(f"ℹ️  Topic {topic} already exists")
                    else:
                        logger.error(f"❌ Failed to create topic {topic}: {e}")
        except Exception as e:
            logger.error(f"❌ Failed to create topics: {e}")

    def delivery_callback(self, err, msg):
        """Enhanced delivery callback with replication awareness"""
        if err is not None:
            self.messages_failed += 1
            logger.error(f"❌ Message delivery failed: {err}")
            self.delivery_callbacks[msg.key()] = {
                'status': 'failed',
                'error': str(err),
                'timestamp': time.time()
            }
        else:
            self.messages_sent += 1
            self.delivery_callbacks[msg.key()] = {
                'status': 'delivered',
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': time.time(),
                'region': self.region
            }
            logger.debug(f"✅ Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")

    def create_post(self, user_id: str, content: str, post_type: str = "text") -> str:
        """Create a new post with replication guarantees"""
        post_id = f"post_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        
        message = {
            'post_id': post_id,
            'user_id': user_id,
            'content': content,
            'post_type': post_type,
            'timestamp': int(time.time() * 1000),
            'region': self.region,
            'metadata': {
                'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'version': '1.0'
            }
        }
        
        try:
            self.producer.produce(
                'critical_posts',
                key=post_id,
                value=json.dumps(message),
                callback=self.delivery_callback,
                headers={'region': self.region, 'type': 'post_creation'}
            )
            logger.info(f"📝 Post queued: {post_id} by {user_id}")
            return post_id
        except Exception as e:
            logger.error(f"❌ Failed to queue post: {e}")
            raise

    def create_engagement(self, post_id: str, user_id: str, action: str) -> str:
        """Create engagement event (like, share, comment)"""
        engagement_id = f"eng_{int(time.time() * 1000)}_{random.randint(100, 999)}"
        
        message = {
            'engagement_id': engagement_id,
            'post_id': post_id,
            'user_id': user_id,
            'action': action,
            'timestamp': int(time.time() * 1000),
            'region': self.region
        }
        
        try:
            self.producer.produce(
                'user_engagement',
                key=engagement_id,
                value=json.dumps(message),
                callback=self.delivery_callback,
                headers={'region': self.region, 'type': 'engagement'}
            )
            logger.info(f"👍 Engagement queued: {action} on {post_id}")
            return engagement_id
        except Exception as e:
            logger.error(f"❌ Failed to queue engagement: {e}")
            raise

    def create_analytics_event(self, event_type: str, data: Dict) -> str:
        """Create analytics event with eventual consistency"""
        event_id = f"analytics_{int(time.time() * 1000)}_{random.randint(10, 99)}"
        
        message = {
            'event_id': event_id,
            'event_type': event_type,
            'data': data,
            'timestamp': int(time.time() * 1000),
            'region': self.region
        }
        
        try:
            self.producer.produce(
                'analytics_events',
                key=event_id,
                value=json.dumps(message),
                callback=self.delivery_callback,
                headers={'region': self.region, 'type': 'analytics'}
            )
            return event_id
        except Exception as e:
            logger.error(f"❌ Failed to queue analytics event: {e}")
            raise

    def simulate_traffic(self, duration_seconds: int = 60):
        """Simulate realistic StreamSocial traffic patterns"""
        logger.info(f"🚀 Starting traffic simulation for {duration_seconds} seconds...")
        
        users = [f"user_{i:04d}" for i in range(1, 101)]
        post_types = ["text", "image", "video", "story"]
        engagement_types = ["like", "share", "comment", "save"]
        
        start_time = time.time()
        posts_created = []
        
        while time.time() - start_time < duration_seconds:
            # Create posts (20% of actions)
            if random.random() < 0.2:
                user = random.choice(users)
                post_type = random.choice(post_types)
                content = f"Amazing {post_type} content from {self.region}!"
                post_id = self.create_post(user, content, post_type)
                posts_created.append(post_id)
            
            # Create engagements (60% of actions)
            elif posts_created and random.random() < 0.6:
                post_id = random.choice(posts_created)
                user = random.choice(users)
                action = random.choice(engagement_types)
                self.create_engagement(post_id, user, action)
            
            # Create analytics events (20% of actions)
            else:
                event_data = {
                    'user_id': random.choice(users),
                    'session_duration': random.randint(30, 3600),
                    'page_views': random.randint(1, 50)
                }
                self.create_analytics_event("user_session", event_data)
            
            # Flush periodically
            if int(time.time()) % 5 == 0:
                self.producer.flush(timeout=1)
            
            # Random delay to simulate natural traffic
            time.sleep(random.uniform(0.1, 1.0))
        
        logger.info(f"✅ Traffic simulation completed. Total messages: {self.messages_sent}")

    def flush_and_close(self):
        """Ensure all messages are delivered before closing"""
        logger.info("🔄 Flushing producer...")
        self.producer.flush(timeout=30)
        logger.info(f"📊 Final stats - Sent: {self.messages_sent}, Failed: {self.messages_failed}")

if __name__ == "__main__":
    # Demo usage
    producer = StreamSocialProducer("localhost:9091,localhost:9092,localhost:9093", "us-east")
    
    try:
        producer.create_topics()
        time.sleep(2)  # Wait for topics to be ready
        
        producer.simulate_traffic(30)
        producer.flush_and_close()
        
    except KeyboardInterrupt:
        logger.info("🛑 Stopping producer...")
        producer.flush_and_close()
