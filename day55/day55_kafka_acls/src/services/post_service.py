"""
Post Service - Handles user post creation with ACL-enforced access
"""
from confluent_kafka import Producer, KafkaException
import json
import time
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import monitor if available
try:
    from monitoring.acl_dashboard import monitor
    MONITORING_ENABLED = True
except ImportError:
    MONITORING_ENABLED = False


class PostService:
    """Service for creating and managing user posts"""
    
    def __init__(self, bootstrap_servers: str, username: str, password: str):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password,
            'client.id': 'post-service'
        })
        self.stats = {
            'posts_created': 0,
            'posts_updated': 0,
            'posts_deleted': 0,
            'access_denied': 0
        }
    
    def create_post(self, user_id: str, content: str) -> Dict[str, Any]:
        """Create a new post"""
        try:
            post = {
                'post_id': f"post_{int(time.time() * 1000)}",
                'user_id': user_id,
                'content': content,
                'timestamp': time.time(),
                'type': 'created'
            }
            
            self.producer.produce(
                'posts.created',
                key=post['post_id'].encode('utf-8'),
                value=json.dumps(post).encode('utf-8')
            )
            self.producer.flush()
            self.stats['posts_created'] += 1
            if MONITORING_ENABLED:
                monitor.record_access('post-service', 'WRITE', 'posts.created', True)
            logger.info(f"✓ Post created: {post['post_id']}")
            return {'status': 'success', 'post': post}
        except KafkaException as e:
            self.stats['access_denied'] += 1
            if MONITORING_ENABLED:
                monitor.record_access('post-service', 'WRITE', 'posts.created', False)
            logger.error(f"✗ Failed to create post: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def update_post(self, post_id: str, new_content: str) -> Dict[str, Any]:
        """Update existing post"""
        try:
            update = {
                'post_id': post_id,
                'content': new_content,
                'timestamp': time.time(),
                'type': 'updated'
            }
            
            self.producer.produce(
                'posts.updated',
                key=post_id.encode('utf-8'),
                value=json.dumps(update).encode('utf-8')
            )
            self.producer.flush()
            self.stats['posts_updated'] += 1
            if MONITORING_ENABLED:
                monitor.record_access('post-service', 'WRITE', 'posts.updated', True)
            logger.info(f"✓ Post updated: {post_id}")
            return {'status': 'success', 'update': update}
        except KafkaException as e:
            self.stats['access_denied'] += 1
            if MONITORING_ENABLED:
                monitor.record_access('post-service', 'WRITE', 'posts.updated', False)
            logger.error(f"✗ Failed to update post: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def test_unauthorized_access(self) -> Dict[str, Any]:
        """Test accessing analytics topic (should fail)"""
        try:
            self.producer.produce(
                'analytics.metrics',
                key=b'test',
                value=b'unauthorized'
            )
            self.producer.flush()
            if MONITORING_ENABLED:
                monitor.record_access('post-service', 'WRITE', 'analytics.metrics', True)
            return {'status': 'error', 'message': 'Access should have been denied!'}
        except KafkaException as e:
            self.stats['access_denied'] += 1
            if MONITORING_ENABLED:
                monitor.record_access('post-service', 'WRITE', 'analytics.metrics', False)
            logger.info(f"✓ Correctly denied access to analytics.metrics")
            return {'status': 'success', 'message': 'Access correctly denied'}
    
    def get_stats(self) -> Dict[str, int]:
        """Get service statistics"""
        return self.stats
