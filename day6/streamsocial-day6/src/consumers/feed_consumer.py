import asyncio
import json
import time
import random
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from typing import Dict, List, Set
import structlog
import redis
import psutil
import os
from dataclasses import dataclass

logger = structlog.get_logger()

@dataclass
class ConsumerMetrics:
    consumer_id: str
    assigned_partitions: List[int]
    messages_processed: int = 0
    processing_rate: float = 0.0
    lag_total: int = 0
    last_update: float = 0.0

class FeedGenerationConsumer:
    def __init__(self, consumer_id: str, group_id: str = "feed-generation-workers"):
        self.consumer_id = consumer_id
        self.group_id = group_id
        self.bootstrap_servers = "localhost:9092"
        
        # Redis for caching social graph
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        # Kafka consumer
        self.consumer = KafkaConsumer(
            'user-activities',
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: int(m.decode('utf-8')) if m else None,
            consumer_timeout_ms=1000
        )
        
        # Producer for generated feeds
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8'),
            acks='all'
        )
        
        # Metrics tracking
        self.metrics = ConsumerMetrics(consumer_id, [])
        self.start_time = time.time()
        self.message_count = 0
        self.last_metrics_time = time.time()
        
    def get_social_graph(self, user_id: int) -> Set[int]:
        """Get user's social connections from cache"""
        cached = self.redis_client.smembers(f"friends:{user_id}")
        if cached:
            return {int(uid) for uid in cached}
        
        # Simulate social graph for demo
        friends = set(random.sample(range(1, 10000), random.randint(50, 200)))
        self.redis_client.sadd(f"friends:{user_id}", *friends)
        self.redis_client.expire(f"friends:{user_id}", 3600)
        return friends
    
    def generate_personalized_feed(self, user_id: int, activity: Dict) -> Dict:
        """Generate personalized feed based on user activity"""
        friends = self.get_social_graph(user_id)
        
        # Simple feed generation logic
        feed_items = []
        if activity['activity_type'] == 'post':
            # Propagate to all friends
            for friend_id in friends:
                feed_items.append({
                    "feed_user_id": friend_id,
                    "content_type": "friend_post",
                    "content_id": activity['content_id'],
                    "author_id": user_id,
                    "score": random.uniform(0.5, 1.0),
                    "timestamp": activity['timestamp']
                })
        
        elif activity['activity_type'] in ['like', 'share', 'comment']:
            # Show engagement to subset of friends
            sample_size = min(len(friends), 20)
            selected_friends = random.sample(list(friends), sample_size)
            
            for friend_id in selected_friends:
                feed_items.append({
                    "feed_user_id": friend_id,
                    "content_type": f"friend_{activity['activity_type']}",
                    "content_id": activity['content_id'],
                    "author_id": user_id,
                    "score": random.uniform(0.3, 0.8),
                    "timestamp": activity['timestamp']
                })
        
        return {
            "generated_feeds": feed_items,
            "processing_time_ms": random.randint(10, 50),
            "consumer_id": self.consumer_id,
            "source_activity": activity
        }
    
    def publish_metrics(self):
        """Publish consumer metrics"""
        current_time = time.time()
        duration = current_time - self.last_metrics_time
        
        if duration >= 5.0:  # Every 5 seconds
            self.metrics.processing_rate = self.message_count / duration
            self.metrics.messages_processed = self.message_count
            self.metrics.last_update = current_time
            
            # Get system metrics
            cpu_percent = psutil.cpu_percent()
            memory_percent = psutil.virtual_memory().percent
            
            metrics_data = {
                "consumer_id": self.consumer_id,
                "group_id": self.group_id,
                "assigned_partitions": self.metrics.assigned_partitions,
                "messages_processed": self.metrics.messages_processed,
                "processing_rate": self.metrics.processing_rate,
                "cpu_percent": cpu_percent,
                "memory_percent": memory_percent,
                "uptime_seconds": current_time - self.start_time,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.producer.send('consumer-metrics', value=metrics_data)
            
            logger.info(
                "Consumer metrics",
                consumer_id=self.consumer_id,
                rate=self.metrics.processing_rate,
                total=self.metrics.messages_processed,
                partitions=self.metrics.assigned_partitions
            )
            
            self.last_metrics_time = current_time
            self.message_count = 0
    
    async def process_messages(self):
        """Main processing loop"""
        logger.info(f"Starting consumer {self.consumer_id} in group {self.group_id}")
        
        try:
            for message in self.consumer:
                # Update partition assignment
                current_partitions = list(self.consumer.assignment())
                self.metrics.assigned_partitions = [tp.partition for tp in current_partitions]
                
                # Process activity
                user_id = message.key
                activity = message.value
                
                # Generate personalized feeds
                feed_result = self.generate_personalized_feed(user_id, activity)
                
                # Publish generated feeds
                for feed_item in feed_result['generated_feeds']:
                    self.producer.send(
                        'generated-feeds',
                        key=feed_item['feed_user_id'],
                        value=feed_item
                    )
                
                # Commit offset
                self.consumer.commit()
                
                self.message_count += 1
                self.publish_metrics()
                
        except KeyboardInterrupt:
            logger.info(f"Shutting down consumer {self.consumer_id}")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    import sys
    import uuid
    
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else f"consumer-{uuid.uuid4().hex[:8]}"
    group_id = sys.argv[2] if len(sys.argv) > 2 else "feed-generation-workers"
    
    consumer = FeedGenerationConsumer(consumer_id, group_id)
    asyncio.run(consumer.process_messages())
