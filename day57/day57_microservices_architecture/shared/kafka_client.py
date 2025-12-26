"""Kafka producer and consumer wrappers"""
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from typing import Callable, Optional
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        self.events_published = 0
    
    def publish(self, topic: str, event: dict, key: Optional[str] = None):
        try:
            future = self.producer.send(topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            self.events_published += 1
            logger.info(f"Published event to {topic}: offset={record_metadata.offset}")
            return record_metadata
        except KafkaError as e:
            logger.error(f"Failed to publish event: {e}")
            raise
    
    def close(self):
        self.producer.flush()
        self.producer.close()

class EventConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            max_poll_records=100
        )
        self.running = False
        self.events_consumed = 0
        self.thread = None
    
    def start(self, handler: Callable[[dict], None]):
        self.running = True
        self.thread = threading.Thread(target=self._consume_loop, args=(handler,))
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Started consumer for topics: {self.consumer.subscription()}")
    
    def _consume_loop(self, handler: Callable[[dict], None]):
        while self.running:
            try:
                messages = self.consumer.poll(timeout_ms=1000)
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            handler(record.value)
                            self.events_consumed += 1
                        except Exception as e:
                            logger.error(f"Error handling message: {e}")
            except Exception as e:
                logger.error(f"Consumer error: {e}")
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        self.consumer.close()
