import json
import time
import threading
from typing import Dict, Callable, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from dataclasses import asdict

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from config.app_config import AppConfig, ProducerConfig
from events.event_types import StreamSocialEvent, EventType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AcksProducerManager:
    def __init__(self):
        self.producers = {}
        self.metrics = {
            'critical': {'sent': 0, 'acked': 0, 'failed': 0, 'latency': []},
            'social': {'sent': 0, 'acked': 0, 'failed': 0, 'latency': []},
            'analytics': {'sent': 0, 'acked': 0, 'failed': 0, 'latency': []}
        }
        self._initialize_producers()
    
    def _initialize_producers(self):
        """Initialize producers with different acknowledgment strategies"""
        configs = {
            'critical': AppConfig.CRITICAL_PRODUCER,
            'social': AppConfig.SOCIAL_PRODUCER,
            'analytics': AppConfig.ANALYTICS_PRODUCER
        }
        
        for producer_type, config in configs.items():
            # Remove enable_idempotence as it's not supported in kafka-python
            # Convert acks string to appropriate type
            acks_value = config.acks
            if acks_value == 'all':
                acks_value = 'all'
            elif acks_value in ['0', '1']:
                acks_value = int(acks_value)
            
            producer_config = {
                'bootstrap_servers': config.bootstrap_servers,
                'acks': acks_value,
                'retries': config.retries,
                'batch_size': config.batch_size,
                'linger_ms': config.linger_ms,
                'max_in_flight_requests_per_connection': config.max_in_flight_requests,
                'compression_type': config.compression_type,
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None
            }
            
            self.producers[producer_type] = KafkaProducer(**producer_config)
            logger.info(f"Initialized {producer_type} producer with acks={config.acks}")
    
    def _get_producer_type(self, event_type: EventType) -> str:
        """Determine producer type based on event classification"""
        critical_events = {
            EventType.USER_REGISTRATION,
            EventType.PAYMENT_PROCESSING,
            EventType.PREMIUM_SUBSCRIPTION
        }
        
        social_events = {
            EventType.POST_CREATION,
            EventType.COMMENT_ADDED,
            EventType.FRIEND_REQUEST,
            EventType.PROFILE_UPDATE
        }
        
        if event_type in critical_events:
            return 'critical'
        elif event_type in social_events:
            return 'social'
        else:
            return 'analytics'
    
    def send_event(self, event: StreamSocialEvent) -> bool:
        """Send event using appropriate producer based on event type"""
        event_type = EventType(event.event_type)
        producer_type = self._get_producer_type(event_type)
        topic = AppConfig.TOPICS[producer_type]
        
        producer = self.producers[producer_type]
        
        start_time = time.time()
        self.metrics[producer_type]['sent'] += 1
        
        try:
            # Send message with callback for acknowledgment tracking
            future = producer.send(
                topic,
                key=event.user_id,
                value=asdict(event)
            )
            
            # Add callback for metrics tracking
            future.add_callback(self._on_send_success, start_time, producer_type)
            future.add_errback(self._on_send_error, producer_type)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to send {producer_type} event: {e}")
            self.metrics[producer_type]['failed'] += 1
            return False
    
    def _on_send_success(self, start_time: float, producer_type: str, record_metadata):
        """Callback for successful message acknowledgment"""
        try:
            latency = (time.time() - start_time) * 1000  # Convert to milliseconds
            self.metrics[producer_type]['acked'] += 1
            self.metrics[producer_type]['latency'].append(latency)
            
            logger.info(f"Message acknowledged for {producer_type} producer "
                       f"(partition: {record_metadata.partition}, "
                       f"offset: {record_metadata.offset}, "
                       f"latency: {latency:.2f}ms)")
        except Exception as e:
            logger.error(f"Error in success callback for {producer_type}: {e}")
    
    def _on_send_error(self, exception, producer_type: str):
        """Callback for failed message send"""
        self.metrics[producer_type]['failed'] += 1
        logger.error(f"Failed to send message with {producer_type} producer: {exception}")
    
    def get_metrics(self) -> Dict:
        """Get current producer metrics"""
        return self.metrics
    
    def flush_all(self):
        """Flush all producers to ensure messages are sent"""
        for producer_type, producer in self.producers.items():
            producer.flush()
            logger.info(f"Flushed {producer_type} producer")
    
    def close_all(self):
        """Close all producers"""
        for producer_type, producer in self.producers.items():
            producer.close()
            logger.info(f"Closed {producer_type} producer")
