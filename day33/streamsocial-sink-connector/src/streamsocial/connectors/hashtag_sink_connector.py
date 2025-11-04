"""
StreamSocial Hashtag Sink Connector
Handles trending hashtags export to PostgreSQL with upsert capability
"""
import json
import logging
import time
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from ..models.hashtag_model import HashtagRecord, HashtagBatch
from ..utils.database_manager import DatabaseManager
from ..utils.metrics_collector import MetricsCollector

@dataclass
class ConnectorConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    database_url: str
    batch_size: int = 1000
    flush_timeout_ms: int = 30000
    retry_backoff_ms: int = 1000
    max_retries: int = 3

class HashtagSinkConnector:
    """Main sink connector for hashtag analytics data"""
    
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.running = False
        self.db_manager = DatabaseManager(config.database_url)
        self.metrics = MetricsCollector()
        self.logger = self._setup_logging()
        self.consumer = None
        self.current_batch = HashtagBatch()
        self.last_flush_time = time.time()
        
    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the connector"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
        
    def start(self):
        """Start the sink connector"""
        self.logger.info("Starting HashtagSinkConnector...")
        self.running = True
        
        # Initialize database schema
        self.db_manager.initialize_schema()
        
        # Setup Kafka consumer
        self._setup_consumer()
        
        # Start processing loop
        self._process_messages()
        
    def stop(self):
        """Stop the sink connector gracefully"""
        self.logger.info("Stopping HashtagSinkConnector...")
        self.running = False
        
        # Flush remaining batch
        if self.current_batch.records:
            self._flush_batch()
            
        # Close consumer
        if self.consumer:
            self.consumer.close()
            
    def _setup_consumer(self):
        """Initialize Kafka consumer"""
        self.logger.info(f"Connecting to Kafka at: {self.config.kafka_bootstrap_servers}")
        consumer_config = {
            'bootstrap.servers': self.config.kafka_bootstrap_servers,
            'group.id': self.config.kafka_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commit for exactly-once
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 10000,
        }
        
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([self.config.kafka_topic])
        self.logger.info(f"Subscribed to topic: {self.config.kafka_topic}")
        
    def _process_messages(self):
        """Main message processing loop"""
        while self.running:
            try:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    self._check_flush_timeout()
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                        
                # Process message
                self._process_single_message(msg)
                
                # Check if batch should be flushed
                if (len(self.current_batch.records) >= self.config.batch_size or 
                    self._should_flush_by_timeout()):
                    self._flush_batch()
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"Error processing messages: {e}")
                self.metrics.increment_error_count()
                time.sleep(self.config.retry_backoff_ms / 1000)
                
    def _process_single_message(self, msg):
        """Process individual Kafka message"""
        try:
            # Parse message
            data = json.loads(msg.value().decode('utf-8'))
            
            # Create hashtag record
            record = HashtagRecord(
                hashtag=data['hashtag'],
                count=data['count'],
                window_start=datetime.fromisoformat(data['window_start']),
                window_end=datetime.fromisoformat(data['window_end']),
                partition=msg.partition(),
                offset=msg.offset()
            )
            
            # Add to current batch
            self.current_batch.add_record(record)
            self.metrics.increment_messages_processed()
            
            self.logger.debug(f"Processed hashtag: {record.hashtag} with count: {record.count}")
            
        except Exception as e:
            self.logger.error(f"Failed to process message: {e}")
            self.metrics.increment_processing_errors()
            
    def _should_flush_by_timeout(self) -> bool:
        """Check if batch should be flushed due to timeout"""
        return (time.time() - self.last_flush_time) * 1000 >= self.config.flush_timeout_ms
        
    def _check_flush_timeout(self):
        """Flush batch if timeout exceeded"""
        if self.current_batch.records and self._should_flush_by_timeout():
            self._flush_batch()
            
    def _flush_batch(self):
        """Flush current batch to database"""
        if not self.current_batch.records:
            return
            
        start_time = time.time()
        retry_count = 0
        
        while retry_count < self.config.max_retries:
            try:
                # Perform upsert operation
                self.db_manager.upsert_hashtags(self.current_batch.records)
                
                # Commit Kafka offsets
                self._commit_offsets()
                
                # Update metrics
                flush_time = time.time() - start_time
                self.metrics.record_batch_flush(len(self.current_batch.records), flush_time)
                
                self.logger.info(f"Flushed batch of {len(self.current_batch.records)} records in {flush_time:.2f}s")
                
                # Reset batch
                self.current_batch = HashtagBatch()
                self.last_flush_time = time.time()
                
                break
                
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Batch flush failed (attempt {retry_count}): {e}")
                
                if retry_count >= self.config.max_retries:
                    self.metrics.increment_batch_failures()
                    # Could implement dead letter queue here
                    raise
                    
                time.sleep(self.config.retry_backoff_ms / 1000 * retry_count)
                
    def _commit_offsets(self):
        """Commit Kafka offsets for processed messages"""
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            self.logger.error(f"Failed to commit offsets: {e}")
            raise
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get connector performance metrics"""
        return self.metrics.get_all_metrics()
