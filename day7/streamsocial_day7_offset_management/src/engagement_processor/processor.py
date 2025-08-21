import asyncio
import json
import time
from typing import Dict, List
from kafka import KafkaConsumer, TopicPartition
import redis.asyncio as aioredis
import asyncpg
import structlog
from config.app_config import AppConfig
from src.offset_manager.offset_coordinator import OffsetCoordinator

logger = structlog.get_logger()

class EngagementEvent:
    def __init__(self, user_id: str, event_type: str, content_id: str, 
                 timestamp: int, metadata: Dict = None):
        self.user_id = user_id
        self.event_type = event_type  # like, share, comment, view
        self.content_id = content_id
        self.timestamp = timestamp
        self.metadata = metadata or {}

class EngagementProcessor:
    def __init__(self, config: AppConfig):
        self.config = config
        self.worker_id = config.worker_id
        self.redis = None
        self.postgres_pool = None
        self.offset_coordinator = None
        self.consumer = None
        self.running = False
        
    async def initialize(self):
        """Initialize processor components"""
        # Initialize Redis
        self.redis = aioredis.from_url(
            f"redis://{self.config.redis.host}:{self.config.redis.port}/{self.config.redis.db}"
        )
        
        # Initialize PostgreSQL
        self.postgres_pool = await asyncpg.create_pool(
            host=self.config.postgres.host,
            port=self.config.postgres.port,
            database=self.config.postgres.database,
            user=self.config.postgres.username,
            password=self.config.postgres.password,
            min_size=5,
            max_size=20
        )
        
        # Initialize offset coordinator
        self.offset_coordinator = OffsetCoordinator(self.redis, self.postgres_pool)
        await self.offset_coordinator.initialize()
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.config.kafka.engagement_topic,
            bootstrap_servers=self.config.kafka.bootstrap_servers,
            group_id=self.config.kafka.consumer_group,
            auto_offset_reset=self.config.kafka.auto_offset_reset,
            enable_auto_commit=False,  # Manual offset management
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info("Engagement processor initialized", worker_id=self.worker_id)
    
    async def process_engagement_batch(self, messages: List) -> List[Dict]:
        """Process a batch of engagement events and return metrics"""
        metrics_batch = []
        
        for message in messages:
            try:
                event_data = message.value
                event = EngagementEvent(**event_data)
                
                # Process different event types
                if event.event_type == "view":
                    duration = event.metadata.get("duration", 1)
                    metrics_batch.append({
                        "user_id": event.user_id,
                        "metric_type": "total_view_time",
                        "metric_value": duration
                    })
                    metrics_batch.append({
                        "user_id": event.user_id,
                        "metric_type": "view_count",
                        "metric_value": 1
                    })
                
                elif event.event_type in ["like", "share", "comment"]:
                    metrics_batch.append({
                        "user_id": event.user_id,
                        "metric_type": f"{event.event_type}_count",
                        "metric_value": 1
                    })
                    
                    # Engagement score calculation
                    score_multiplier = {"like": 1, "share": 3, "comment": 2}
                    metrics_batch.append({
                        "user_id": event.user_id,
                        "metric_type": "engagement_score",
                        "metric_value": score_multiplier.get(event.event_type, 1)
                    })
                
            except Exception as e:
                logger.error("Failed to process event", error=str(e), event=message.value)
        
        return metrics_batch
    
    async def start_processing(self):
        """Start the main processing loop"""
        self.running = True
        logger.info("Starting engagement processing", worker_id=self.worker_id)
        
        # Start heartbeat task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        try:
            while self.running:
                try:
                    # Poll for messages
                    message_pack = self.consumer.poll(timeout_ms=1000, max_records=self.config.batch_size)
                    
                    if not message_pack:
                        continue
                    
                    # Process each partition
                    for topic_partition, messages in message_pack.items():
                        await self._process_partition_batch(topic_partition, messages)
                
                except Exception as e:
                    logger.error("Processing error", error=str(e))
                    await asyncio.sleep(1)
        
        finally:
            heartbeat_task.cancel()
            logger.info("Engagement processing stopped", worker_id=self.worker_id)
    
    async def _process_partition_batch(self, topic_partition: TopicPartition, messages: List):
        """Process a batch of messages from a specific partition"""
        if not messages:
            return
        
        partition = topic_partition.partition
        start_offset = messages[0].offset
        
        # Claim offset range
        offset_range = await self.offset_coordinator.claim_offset_range(
            self.worker_id, partition, start_offset, len(messages)
        )
        
        if not offset_range:
            logger.warning("Failed to claim offset range", 
                          partition=partition, start_offset=start_offset)
            return
        
        range_key = f"offset_range:{partition}:{start_offset}"
        
        try:
            # Update status to processing
            await self.offset_coordinator.update_range_status(range_key, "processing")
            
            # Process the batch
            metrics_data = await self.process_engagement_batch(messages)
            
            # Update status to completed
            await self.offset_coordinator.update_range_status(range_key, "completed")
            
            # Commit the batch atomically
            await self.offset_coordinator.commit_offset_range(range_key, metrics_data)
            
            logger.info("Batch processed successfully", 
                       partition=partition, 
                       start_offset=start_offset,
                       end_offset=messages[-1].offset,
                       metrics_count=len(metrics_data))
        
        except Exception as e:
            logger.error("Batch processing failed", 
                        error=str(e), 
                        partition=partition, 
                        start_offset=start_offset)
            await self.offset_coordinator.update_range_status(range_key, "failed")
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats"""
        while self.running:
            try:
                await self.offset_coordinator.worker_heartbeat(self.worker_id)
                await asyncio.sleep(self.config.heartbeat_interval / 1000)
            except Exception as e:
                logger.error("Heartbeat failed", error=str(e))
    
    async def stop(self):
        """Stop the processor"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.postgres_pool:
            await self.postgres_pool.close()
        if self.redis:
            await self.redis.close()
