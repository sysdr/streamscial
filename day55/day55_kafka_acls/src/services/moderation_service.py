"""
Moderation Service - Monitors and moderates posts with special ACL privileges
"""
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import json
import logging
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ModerationService:
    """Service for moderating content with elevated permissions"""
    
    def __init__(self, bootstrap_servers: str, username: str, password: str):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password,
            'group.id': 'moderation-scanner',
            'auto.offset.reset': 'earliest'
        })
        
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password
        })
        
        self.admin = AdminClient({
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': username,
            'sasl.password': password
        })
        
        self.stats = {
            'posts_scanned': 0,
            'flags_created': 0,
            'deletions_requested': 0,
            'access_denied': 0
        }
    
    def subscribe_to_posts(self):
        """Subscribe to all post topics for monitoring"""
        try:
            self.consumer.subscribe(['posts.created', 'posts.updated'])
            logger.info("✓ Subscribed to post topics for moderation")
        except KafkaException as e:
            logger.error(f"✗ Failed to subscribe: {e}")
    
    def scan_posts(self, count: int = 10) -> List[Dict[str, Any]]:
        """Scan posts for moderation"""
        flagged_posts = []
        
        try:
            for _ in range(count):
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                
                post = json.loads(msg.value().decode('utf-8'))
                self.stats['posts_scanned'] += 1
                
                # Simple content filter
                content = post.get('content', '').lower()
                if any(word in content for word in ['spam', 'inappropriate', 'banned']):
                    flag = {
                        'post_id': post.get('post_id'),
                        'reason': 'content_violation',
                        'severity': 'high',
                        'timestamp': post.get('timestamp')
                    }
                    
                    self.producer.produce(
                        'moderation.flags',
                        key=post.get('post_id', '').encode('utf-8'),
                        value=json.dumps(flag).encode('utf-8')
                    )
                    self.stats['flags_created'] += 1
                    flagged_posts.append(flag)
            
            self.producer.flush()
            logger.info(f"✓ Scanned {count} posts, flagged {len(flagged_posts)}")
        except KafkaException as e:
            self.stats['access_denied'] += 1
            logger.error(f"✗ Moderation error: {e}")
        
        return flagged_posts
    
    def request_deletion(self, post_id: str) -> Dict[str, Any]:
        """Request post deletion (moderation privilege)"""
        try:
            deletion = {
                'post_id': post_id,
                'action': 'delete',
                'reason': 'moderation_action',
                'timestamp': time.time()
            }
            
            self.producer.produce(
                'posts.deleted',
                key=post_id.encode('utf-8'),
                value=json.dumps(deletion).encode('utf-8')
            )
            self.producer.flush()
            self.stats['deletions_requested'] += 1
            logger.info(f"✓ Deletion requested for: {post_id}")
            return {'status': 'success', 'deletion': deletion}
        except KafkaException as e:
            self.stats['access_denied'] += 1
            logger.error(f"✗ Failed to request deletion: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def get_stats(self) -> Dict[str, int]:
        """Get service statistics"""
        return self.stats
    
    def close(self):
        """Close consumer"""
        self.consumer.close()


import time
