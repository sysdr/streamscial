"""Consumer for StreamSocial timeline topics with transaction isolation."""

import json
import logging
from typing import Dict, Any, Callable, Optional
from dataclasses import dataclass
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException

from config.kafka_config import KafkaConfig

logger = logging.getLogger(__name__)

@dataclass
class ConsumedMessage:
    topic: str
    partition: int
    offset: int
    key: str
    value: Dict[str, Any]
    timestamp: datetime
    headers: Optional[Dict[str, str]] = None

class StreamSocialTimelineConsumer:
    """Consumer for reading committed transactions from StreamSocial topics."""
    
    def __init__(self, consumer_group: str = "timeline-readers"):
        self.consumer_config = KafkaConfig.CONSUMER_CONFIG.copy()
        self.consumer_config.update({
            'group.id': consumer_group,
            'isolation.level': 'read_committed',  # Only read committed transactions
            'enable.auto.commit': False,
        })
        
        self.consumer = Consumer(self.consumer_config)
        
        self.topics = list(KafkaConfig.TOPICS.values())
        self.consumer.subscribe(self.topics)
        
        self.message_handlers: Dict[str, Callable[[ConsumedMessage], None]] = {}
        self.consumed_messages = []
        
        logger.info(f"Timeline consumer initialized for group: {consumer_group}")
    
    def register_handler(self, topic: str, handler: Callable[[ConsumedMessage], None]):
        """Register a message handler for a specific topic."""
        self.message_handlers[topic] = handler
        logger.info(f"Registered handler for topic: {topic}")
    
    def consume_messages(self, timeout_ms: int = 1000, max_messages: int = 100) -> int:
        """Consume messages from subscribed topics."""
        consumed_count = 0
        
        try:
            # Poll for messages with timeout
            msg = self.consumer.poll(timeout=timeout_ms / 1000.0)
            
            while msg is not None and consumed_count < max_messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        logger.debug(f"Reached end of partition {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                else:
                    # Process the message
                    consumed_message = ConsumedMessage(
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        key=msg.key().decode('utf-8') if msg.key() else None,
                        value=json.loads(msg.value().decode('utf-8')) if msg.value() else {},
                        timestamp=datetime.fromtimestamp(msg.timestamp()[1] / 1000) if msg.timestamp()[0] else datetime.utcnow(),
                        headers={k: v.decode() if isinstance(v, bytes) else v for k, v in (msg.headers() or [])}
                    )
                    
                    self.consumed_messages.append(consumed_message)
                    consumed_count += 1
                    
                    # Call registered handler if available
                    if consumed_message.topic in self.message_handlers:
                        try:
                            self.message_handlers[consumed_message.topic](consumed_message)
                        except Exception as e:
                            logger.error(f"Handler error for topic {consumed_message.topic}: {e}")
                    
                    logger.debug(f"Consumed message from {consumed_message.topic}: {consumed_message.key}")
                
                # Poll for next message
                msg = self.consumer.poll(timeout=0.1)
            
            # Commit offsets after processing
            if consumed_count > 0:
                self.consumer.commit()
                logger.info(f"Committed offsets for {consumed_count} messages")
        
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        
        return consumed_count
    
    def get_consumer_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics and status."""
        return {
            'group_id': self.consumer_config['group.id'],
            'subscribed_topics': self.topics,
            'consumed_messages_count': len(self.consumed_messages),
            'registered_handlers': list(self.message_handlers.keys()),
            'consumer_metrics': {}
        }
    
    def close(self):
        """Close the consumer."""
        self.consumer.close()
        logger.info("Timeline consumer closed")

# Message handlers for different topic types
def handle_timeline_message(message: ConsumedMessage):
    """Handle user timeline messages."""
    logger.info(f"Timeline update for user: {message.value.get('user_id')} - {message.value.get('content', '')[:50]}")

def handle_global_feed_message(message: ConsumedMessage):
    """Handle global feed messages.""" 
    logger.info(f"Global feed post: {message.value.get('post_id')} by {message.value.get('username')}")

def handle_notification_message(message: ConsumedMessage):
    """Handle notification messages."""
    logger.info(f"Notification for user {message.value.get('user_id')}: {message.value.get('title')}")

def handle_analytics_message(message: ConsumedMessage):
    """Handle analytics messages."""
    logger.info(f"Analytics event: {message.value.get('event_type')} for user {message.value.get('user_id')}")
