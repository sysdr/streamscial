"""SASL-authenticated Kafka consumer for StreamSocial."""

import json
from typing import Dict, Callable, Optional
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError


class SALSConsumer:
    """Kafka consumer with SASL authentication."""
    
    def __init__(self, bootstrap_servers: str, group_id: str, 
                 username: str, password: str, mechanism: str = 'PLAIN'):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.username = username
        self.mechanism = mechanism
        
        sasl_config = {
            'bootstrap_servers': bootstrap_servers,
            'group_id': group_id,
            'security_protocol': 'SASL_PLAINTEXT',
            'sasl_mechanism': mechanism,
            'sasl_plain_username': username,
            'sasl_plain_password': password,
            'value_deserializer': lambda v: json.loads(v.decode('utf-8')),
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True
        }
        
        self.consumer = None
        self.stats = {
            'messages_consumed': 0,
            'auth_failures': 0,
            'processing_errors': 0,
            'last_message_time': None
        }
        
        try:
            self.consumer = KafkaConsumer(**sasl_config)
        except Exception as e:
            if 'authentication' in str(e).lower():
                self.stats['auth_failures'] += 1
            raise
    
    def subscribe(self, topics: list):
        """Subscribe to topics."""
        if self.consumer:
            self.consumer.subscribe(topics)
    
    def consume(self, callback: Optional[Callable] = None, 
                duration_seconds: int = 30):
        """Consume messages for specified duration."""
        if not self.consumer:
            raise RuntimeError("Consumer not initialized")
        
        print(f"📥 Consuming messages with {self.mechanism} auth for {duration_seconds}s...")
        start_time = datetime.now()
        
        try:
            for message in self.consumer:
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > duration_seconds:
                    break
                
                try:
                    if callback:
                        callback(message.value)
                    
                    self.stats['messages_consumed'] += 1
                    self.stats['last_message_time'] = datetime.now().isoformat()
                    
                    if self.stats['messages_consumed'] % 20 == 0:
                        print(f"   Consumed {self.stats['messages_consumed']} messages...")
                
                except Exception as e:
                    self.stats['processing_errors'] += 1
                    print(f"⚠️ Processing error: {e}")
        
        except KafkaError as e:
            if 'authentication' in str(e).lower():
                self.stats['auth_failures'] += 1
            raise
        
        print(f"✅ Consumed {self.stats['messages_consumed']} messages")
    
    def get_stats(self) -> Dict:
        """Get consumer statistics."""
        return self.stats.copy()
    
    def close(self):
        """Close consumer connection."""
        if self.consumer:
            self.consumer.close()
