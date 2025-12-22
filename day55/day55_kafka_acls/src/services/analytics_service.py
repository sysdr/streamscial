"""
Analytics Service - Reads posts for analytics with ACL-enforced access
"""
from confluent_kafka import Consumer, Producer, KafkaException
import json
import logging
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalyticsService:
    """Service for analyzing posts and generating metrics"""
    
    def __init__(self, bootstrap_servers: str, username: str, password: str):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password,
            'group.id': 'analytics-aggregator',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        })
        
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password,
            'client.id': 'analytics-service'
        })
        
        self.stats = {
            'posts_read': 0,
            'metrics_generated': 0,
            'access_denied': 0
        }
    
    def subscribe_to_posts(self):
        """Subscribe to post topics"""
        try:
            self.consumer.subscribe(['posts.created', 'posts.updated'])
            logger.info("✓ Subscribed to post topics")
        except KafkaException as e:
            logger.error(f"✗ Failed to subscribe: {e}")
    
    def process_posts(self, count: int = 10) -> List[Dict[str, Any]]:
        """Process posts and generate analytics"""
        posts_processed = []
        
        try:
            for _ in range(count):
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                post = json.loads(msg.value().decode('utf-8'))
                posts_processed.append(post)
                self.stats['posts_read'] += 1
                
                # Generate metric
                metric = {
                    'post_id': post.get('post_id'),
                    'user_id': post.get('user_id'),
                    'content_length': len(post.get('content', '')),
                    'timestamp': post.get('timestamp')
                }
                
                self.producer.produce(
                    'analytics.metrics',
                    key=post.get('post_id', '').encode('utf-8'),
                    value=json.dumps(metric).encode('utf-8')
                )
                self.stats['metrics_generated'] += 1
            
            self.producer.flush()
            logger.info(f"✓ Processed {len(posts_processed)} posts")
        except KafkaException as e:
            self.stats['access_denied'] += 1
            logger.error(f"✗ Access error: {e}")
        
        return posts_processed
    
    def test_unauthorized_access(self) -> Dict[str, Any]:
        """Test writing to posts topic (should fail)"""
        try:
            self.producer.produce(
                'posts.created',
                key=b'test',
                value=b'unauthorized'
            )
            self.producer.flush()
            return {'status': 'error', 'message': 'Access should have been denied!'}
        except KafkaException as e:
            self.stats['access_denied'] += 1
            logger.info(f"✓ Correctly denied write access to posts.created")
            return {'status': 'success', 'message': 'Access correctly denied'}
    
    def get_stats(self) -> Dict[str, int]:
        """Get service statistics"""
        return self.stats
    
    def close(self):
        """Close consumer"""
        self.consumer.close()
