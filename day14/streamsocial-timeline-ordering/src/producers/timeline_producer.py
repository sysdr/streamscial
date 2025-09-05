import json
import time
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import structlog
from config.kafka_config import kafka_config

logger = structlog.get_logger(__name__)

class TimelineMessage:
    def __init__(self, user_id: str, post_id: str, content: str, timestamp: Optional[float] = None):
        self.user_id = user_id
        self.post_id = post_id
        self.content = content
        self.timestamp = timestamp or time.time()
        self.created_at = datetime.fromtimestamp(self.timestamp).isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'user_id': self.user_id,
            'post_id': self.post_id,
            'content': self.content,
            'timestamp': self.timestamp,
            'created_at': self.created_at
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

class TimelineProducer:
    def __init__(self):
        self.producer = KafkaProducer(**kafka_config.producer_config)
        self.topic = kafka_config.topic_name
        self.sent_messages = []
        logger.info("Timeline producer initialized")
    
    def calculate_partition_key(self, user_id: str) -> str:
        """Calculate consistent partition key for user ordering"""
        return f"user:{user_id}"
    
    def get_partition_for_key(self, key: str) -> int:
        """Calculate which partition a key maps to"""
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_value % kafka_config.partitions
    
    def send_timeline_message(self, message: TimelineMessage) -> bool:
        """Send timeline message with user-based partition key"""
        try:
            partition_key = self.calculate_partition_key(message.user_id)
            target_partition = self.get_partition_for_key(partition_key)
            
            future = self.producer.send(
                topic=self.topic,
                key=partition_key,
                value=message.to_json(),
                partition=target_partition
            )
            
            # Wait for acknowledgment
            record_metadata = future.get(timeout=10)
            
            # Track sent message for verification
            self.sent_messages.append({
                'message': message.to_dict(),
                'partition_key': partition_key,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset
            })
            
            logger.info(
                "Message sent successfully",
                user_id=message.user_id,
                post_id=message.post_id,
                partition=record_metadata.partition,
                offset=record_metadata.offset
            )
            
            return True
            
        except KafkaError as e:
            logger.error("Failed to send message", error=str(e))
            return False
    
    def send_bulk_timeline_messages(self, messages: list[TimelineMessage]) -> Dict[str, int]:
        """Send multiple timeline messages and return statistics"""
        stats = {'success': 0, 'failed': 0, 'partitions_used': set()}
        
        for message in messages:
            if self.send_timeline_message(message):
                stats['success'] += 1
                partition_key = self.calculate_partition_key(message.user_id)
                partition = self.get_partition_for_key(partition_key)
                stats['partitions_used'].add(partition)
            else:
                stats['failed'] += 1
        
        stats['partitions_used'] = len(stats['partitions_used'])
        return stats
    
    def get_sent_messages_by_user(self, user_id: str) -> list:
        """Get all sent messages for a specific user in order"""
        user_messages = [
            msg for msg in self.sent_messages 
            if msg['message']['user_id'] == user_id
        ]
        return sorted(user_messages, key=lambda x: x['message']['timestamp'])
    
    def close(self):
        """Close producer connection"""
        self.producer.close()
        logger.info("Timeline producer closed")
