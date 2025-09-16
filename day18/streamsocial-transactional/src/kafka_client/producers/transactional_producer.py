"""Transactional Kafka producer for StreamSocial atomic operations."""

import json
import uuid
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
from contextlib import contextmanager

from confluent_kafka import Producer, KafkaError

from config.kafka_config import KafkaConfig
from src.streamsocial.models.post import Post, Notification, AnalyticsEvent

logger = logging.getLogger(__name__)

class TransactionError(Exception):
    """Custom exception for transaction-related errors."""
    pass

class StreamSocialTransactionalProducer:
    """High-performance transactional producer for StreamSocial atomic operations."""
    
    def __init__(self, transaction_id_prefix: str = "streamsocial"):
        self.transaction_id = f"{transaction_id_prefix}-{uuid.uuid4().hex[:8]}"
        
        # Configure producer with transactional settings
        self.producer_config = KafkaConfig.PRODUCER_CONFIG.copy()
        self.producer_config.update({
            'transactional.id': self.transaction_id,
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 2147483647,
            'max.in.flight.requests.per.connection': 5,
            'transaction.timeout.ms': KafkaConfig.TRANSACTION_TIMEOUT_MS,
        })
        
        self.producer = Producer(self.producer_config)
        
        # Initialize transactional producer
        self.producer.init_transactions()
        
        self.topics = KafkaConfig.TOPICS
        self.transaction_active = False
        self.message_buffer = []
        
        logger.info(f"Initialized transactional producer with ID: {self.transaction_id}")
    
    @contextmanager
    def transaction(self):
        """Context manager for transactional operations."""
        if self.transaction_active:
            raise TransactionError("Transaction already active")
            
        self.producer.begin_transaction()
        self.transaction_active = True
        
        try:
            logger.info("Transaction started")
            yield self
            
            # Commit transaction if no exceptions
            self.producer.commit_transaction()
            logger.info("Transaction committed successfully")
            
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            self.producer.abort_transaction()
            raise TransactionError(f"Transaction aborted: {e}")
            
        finally:
            self.transaction_active = False
            self.message_buffer.clear()
    
    def _send_message(self, topic: str, key: str, value: Dict[str, Any], 
                     partition: Optional[int] = None) -> None:
        """Send message within transaction context."""
        if not self.transaction_active:
            raise TransactionError("No active transaction. Use transaction() context manager.")
            
        try:
            # Serialize the value to JSON
            serialized_value = json.dumps(value).encode('utf-8')
            serialized_key = key.encode('utf-8') if key else None
            
            # Send message using confluent-kafka
            self.producer.produce(
                topic=topic,
                key=serialized_key,
                value=serialized_value,
                partition=partition if partition is not None else -1,  # Let Kafka choose partition
                callback=self._delivery_callback
            )
            
            self.message_buffer.append({
                'topic': topic,
                'key': key,
                'value': value,
                'timestamp': datetime.utcnow()
            })
            
            logger.debug(f"Message queued for topic: {topic}, key: {key}")
            
        except Exception as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            raise TransactionError(f"Message send failed: {e}")
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def atomic_post_creation(self, post: Post, followers: List[str]) -> bool:
        """Atomically create a post across timeline, global feed, and notifications."""
        
        try:
            with self.transaction():
                # 1. Add to user's timeline
                timeline_key = f"user-{post.user_id}"
                self._send_message(
                    topic=self.topics['user_timeline'],
                    key=timeline_key,
                    value=post.to_dict()
                )
                
                # 2. Add to global feed
                feed_key = f"post-{post.post_id}"
                self._send_message(
                    topic=self.topics['global_feed'],
                    key=feed_key,
                    value=post.to_dict()
                )
                
                # 3. Create notifications for followers
                for follower_id in followers[:100]:  # Limit to prevent transaction timeout
                    notification = Notification(
                        notification_id=f"notif-{uuid.uuid4().hex[:12]}",
                        user_id=follower_id,
                        type="new_post",
                        title="New Post",
                        message=f"{post.username} posted: {post.content[:50]}...",
                        post_id=post.post_id,
                        from_user_id=post.user_id,
                        created_at=datetime.utcnow()
                    )
                    
                    self._send_message(
                        topic=self.topics['notifications'],
                        key=f"user-{follower_id}",
                        value=notification.to_dict()
                    )
                
                # 4. Analytics event
                analytics_event = AnalyticsEvent(
                    event_id=f"analytics-{uuid.uuid4().hex[:12]}",
                    event_type="post_created",
                    user_id=post.user_id,
                    post_id=post.post_id,
                    timestamp=datetime.utcnow(),
                    metadata={
                        'content_type': post.content_type.value,
                        'hashtags_count': len(post.hashtags or []),
                        'mentions_count': len(post.mentions or [])
                    }
                )
                
                self._send_message(
                    topic=self.topics['analytics'],
                    key=f"event-{analytics_event.event_id}",
                    value=analytics_event.to_dict()
                )
            
            logger.info(f"Atomic post creation successful for post: {post.post_id}")
            return True
            
        except TransactionError as e:
            logger.error(f"Atomic post creation failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in atomic post creation: {e}")
            return False
    
    def atomic_multi_user_timeline_update(self, post: Post, target_users: List[str]) -> bool:
        """Atomically update multiple users' timelines (for assignment)."""
        
        try:
            with self.transaction():
                # Update all target users' timelines atomically
                for user_id in target_users:
                    timeline_key = f"user-{user_id}"
                    
                    # Create timeline entry with follower-specific data
                    timeline_entry = post.to_dict()
                    timeline_entry.update({
                        'timeline_user_id': user_id,
                        'visibility': 'follower_feed',
                        'delivered_at': datetime.utcnow().isoformat()
                    })
                    
                    self._send_message(
                        topic=self.topics['user_timeline'],
                        key=timeline_key,
                        value=timeline_entry
                    )
                
                # Analytics for multi-user update
                analytics_event = AnalyticsEvent(
                    event_id=f"analytics-{uuid.uuid4().hex[:12]}",
                    event_type="multi_timeline_update",
                    user_id=post.user_id,
                    post_id=post.post_id,
                    timestamp=datetime.utcnow(),
                    metadata={
                        'target_users_count': len(target_users),
                        'update_type': 'follower_timeline_sync'
                    }
                )
                
                self._send_message(
                    topic=self.topics['analytics'],
                    key=f"event-{analytics_event.event_id}",
                    value=analytics_event.to_dict()
                )
            
            logger.info(f"Multi-user timeline update successful for {len(target_users)} users")
            return True
            
        except TransactionError as e:
            logger.error(f"Multi-user timeline update failed: {e}")
            return False
    
    def get_transaction_metrics(self) -> Dict[str, Any]:
        """Get transaction producer metrics."""
        return {
            'transaction_id': self.transaction_id,
            'transaction_active': self.transaction_active,
            'buffered_messages': len(self.message_buffer),
            'topics': list(self.topics.keys()),
            'producer_metrics': {
                'status': 'active' if self.producer else 'inactive',
                'transaction_id': self.transaction_id
            }
        }
    
    def close(self):
        """Close the producer and clean up resources."""
        if self.transaction_active:
            logger.warning("Closing producer with active transaction - aborting")
            self.producer.abort_transaction()
            
        # Flush any remaining messages and close the producer
        self.producer.flush(timeout=10.0)
        logger.info("Transactional producer closed")

# Factory function for easy instantiation
def create_transactional_producer(instance_id: Optional[str] = None) -> StreamSocialTransactionalProducer:
    """Create a new transactional producer instance."""
    prefix = f"streamsocial-{instance_id}" if instance_id else "streamsocial"
    return StreamSocialTransactionalProducer(transaction_id_prefix=prefix)
