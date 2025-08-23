import asyncio
import json
import signal
import sys
from typing import List, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException
from datetime import datetime
import structlog
import uuid
from contextlib import contextmanager

from ..models.engagement import EngagementEvent
from ..utils.database import DatabaseManager
from ..monitoring.metrics import MetricsCollector

logger = structlog.get_logger()

class ReliableEngagementConsumer:
    def __init__(self, config: dict, db_manager: DatabaseManager, metrics: MetricsCollector):
        self.config = config
        self.db_manager = db_manager
        self.metrics = metrics
        self.consumer = None
        self.running = False
        self.batch_size = config.get('batch_size', 50)
        self.max_processing_time = config.get('max_processing_time', 30)
        
    def create_consumer(self):
        """Create Kafka consumer with manual commit configuration"""
        consumer_config = {
            'bootstrap.servers': self.config['bootstrap_servers'],
            'group.id': self.config['group_id'],
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Manual commits only
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
        }
        
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([self.config['topic']])
        logger.info("Consumer created with manual commits", group_id=self.config['group_id'])
    
    @contextmanager
    def transaction_context(self):
        """Database transaction context for atomic processing"""
        session = self.db_manager.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error("Transaction rolled back", error=str(e))
            raise
        finally:
            session.close()
    
    def process_engagement_batch(self, messages: List) -> List[EngagementEvent]:
        """Process batch of engagement messages with validation"""
        processed_events = []
        failed_events = []
        
        for msg in messages:
            try:
                # Parse engagement event
                engagement_data = json.loads(msg.value().decode('utf-8'))
                engagement_event = EngagementEvent.from_dict(engagement_data)
                
                # Add unique event ID if missing
                if not engagement_event.event_id:
                    engagement_event.event_id = str(uuid.uuid4())
                
                # Validate engagement
                if self.validate_engagement(engagement_event):
                    processed_events.append(engagement_event)
                    self.metrics.increment_counter('engagements_processed')
                else:
                    failed_events.append(engagement_event)
                    self.metrics.increment_counter('engagements_failed')
                    
            except Exception as e:
                logger.error("Failed to process message", error=str(e), offset=msg.offset())
                self.metrics.increment_counter('processing_errors')
                failed_events.append(None)
        
        logger.info(f"Batch processed: {len(processed_events)} success, {len(failed_events)} failed")
        return processed_events
    
    def validate_engagement(self, event: EngagementEvent) -> bool:
        """Validate engagement event data"""
        required_fields = ['user_id', 'content_id', 'engagement_type']
        valid_types = ['like', 'share', 'comment', 'follow', 'view']
        
        # Check required fields
        for field in required_fields:
            if not getattr(event, field):
                logger.warning("Missing required field", field=field, event_id=event.event_id)
                return False
        
        # Check engagement type
        if event.engagement_type not in valid_types:
            logger.warning("Invalid engagement type", type=event.engagement_type)
            return False
        
        # Check for duplicates (idempotency)
        if self.db_manager.is_duplicate(event.event_id):
            logger.info("Duplicate engagement detected", event_id=event.event_id)
            return False
        
        return True
    
    def store_engagement_batch(self, events: List[EngagementEvent]) -> bool:
        """Store batch of engagements atomically"""
        try:
            with self.transaction_context() as session:
                for event in events:
                    self.db_manager.store_engagement(event, session)
                
                logger.info(f"Batch stored successfully: {len(events)} engagements")
                self.metrics.increment_counter('engagements_stored', len(events))
                return True
                
        except Exception as e:
            logger.error("Failed to store engagement batch", error=str(e))
            self.metrics.increment_counter('storage_errors')
            return False
    
    def commit_offsets(self, messages: List) -> bool:
        """Manually commit offsets after successful processing"""
        try:
            if messages:
                # Commit the last message's offset + 1
                last_msg = messages[-1]
                self.consumer.commit(message=last_msg, asynchronous=False)
                logger.info(f"Committed offset: {last_msg.offset()}", 
                           partition=last_msg.partition(), 
                           topic=last_msg.topic())
                self.metrics.record_commit_lag(last_msg.offset())
                return True
        except KafkaException as e:
            logger.error("Failed to commit offsets", error=str(e))
            self.metrics.increment_counter('commit_errors')
            return False
        
        return False
    
    async def consume_loop(self):
        """Main consumption loop with manual commit strategy"""
        logger.info("Starting reliable consumption loop")
        
        while self.running:
            try:
                # Poll for messages
                messages = self.consumer.consume(num_messages=self.batch_size, timeout=5.0)
                
                if not messages:
                    await asyncio.sleep(0.1)
                    continue
                
                # Filter out error messages
                valid_messages = [msg for msg in messages if not msg.error()]
                error_messages = [msg for msg in messages if msg.error()]
                
                # Log errors
                for msg in error_messages:
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error", error=msg.error())
                
                if not valid_messages:
                    continue
                
                logger.info(f"Processing batch of {len(valid_messages)} messages")
                
                # Process engagements
                processed_events = self.process_engagement_batch(valid_messages)
                
                if processed_events:
                    # Store in database atomically
                    if self.store_engagement_batch(processed_events):
                        # Only commit if storage succeeded
                        if self.commit_offsets(valid_messages):
                            logger.info("Batch processed and committed successfully")
                        else:
                            logger.error("Storage succeeded but commit failed - potential duplicate processing")
                    else:
                        logger.error("Storage failed - offsets not committed, will retry")
                else:
                    # Even if no events processed, commit offsets for invalid messages
                    self.commit_offsets(valid_messages)
                    logger.info("No valid events to process, committed offsets")
                
            except KeyboardInterrupt:
                logger.info("Shutdown signal received")
                break
            except Exception as e:
                logger.error("Unexpected error in consume loop", error=str(e))
                self.metrics.increment_counter('consume_errors')
                await asyncio.sleep(1)
    
    async def start(self):
        """Start the reliable consumer"""
        self.running = True
        self.create_consumer()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        try:
            await self.consume_loop()
        finally:
            await self.stop()
    
    async def stop(self):
        """Gracefully stop the consumer"""
        logger.info("Stopping reliable consumer")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}")
        self.running = False
