import json
import time
from typing import Dict, List, Optional
from kafka import KafkaProducer, KafkaConsumer
from dataclasses import dataclass, asdict
import logging

@dataclass
class DLQMessage:
    original_topic: str
    partition: int
    offset: int
    key: str
    value: bytes
    timestamp: int
    error_type: str
    error_message: str
    attempt_count: int
    headers: Dict[str, str]

class DLQManager:
    def __init__(self, bootstrap_servers: str, dlq_topic: str = "streamsocial-dlq"):
        self.bootstrap_servers = bootstrap_servers
        self.dlq_topic = dlq_topic
        self.logger = logging.getLogger(__name__)
        
        # Initialize producer for DLQ messages
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=3,
            acks='all'
        )
        
    def send_to_dlq(self, message: DLQMessage):
        """Send failed message to dead letter queue"""
        try:
            dlq_payload = {
                'original_topic': message.original_topic,
                'partition': message.partition,
                'offset': message.offset,
                'key': message.key,
                'value': message.value.decode('utf-8', errors='ignore'),
                'timestamp': message.timestamp,
                'error_type': message.error_type,
                'error_message': message.error_message,
                'attempt_count': message.attempt_count,
                'dlq_timestamp': int(time.time()),
                'headers': message.headers
            }
            
            self.producer.send(
                self.dlq_topic,
                value=dlq_payload,
                key=message.key
            )
            
            self.logger.info(f"Sent message to DLQ: {message.key}")
            
        except Exception as e:
            self.logger.error(f"Failed to send message to DLQ: {e}")
            
    def process_dlq_batch(self, max_messages: int = 100) -> List[Dict]:
        """Process batch of DLQ messages for recovery"""
        consumer = KafkaConsumer(
            self.dlq_topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000,
            max_poll_records=max_messages
        )
        
        messages = []
        try:
            for message in consumer:
                messages.append(message.value)
                if len(messages) >= max_messages:
                    break
                    
        except Exception as e:
            self.logger.error(f"Error processing DLQ batch: {e}")
            
        finally:
            consumer.close()
            
        return messages
        
    def retry_dlq_message(self, dlq_message: Dict, target_topic: str):
        """Retry a message from DLQ to original topic"""
        try:
            retry_payload = {
                'content': dlq_message['value'],
                'retry_attempt': dlq_message.get('attempt_count', 0) + 1,
                'original_error': dlq_message['error_message']
            }
            
            self.producer.send(
                target_topic,
                value=json.dumps(retry_payload),
                key=dlq_message['key']
            )
            
            self.logger.info(f"Retried DLQ message: {dlq_message['key']}")
            
        except Exception as e:
            self.logger.error(f"Failed to retry DLQ message: {e}")
