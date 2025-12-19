"""Engagement consumer with structured logging and error scenarios"""
import json
import time
import random
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import redis
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from shared.structured_logger import setup_structured_logger, LogContext

class EngagementConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'viral-content'
        self.logger = setup_structured_logger(
            'engagement-consumer',
            'logs/application/consumer.log'
        )
        
        # Initialize consumer
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'engagement-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000
        })
        
        # Initialize Redis for caching
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, 
                                           decode_responses=True, socket_timeout=1)
            self.redis_client.ping()
            self.logger.info("redis_connected")
        except Exception as e:
            self.logger.warning("redis_unavailable", error=str(e))
            self.redis_client = None
        
        self.consumer.subscribe([self.topic])
        self.logger.info("consumer_initialized",
                        bootstrap_servers=bootstrap_servers,
                        topic=self.topic,
                        group_id='engagement-group')
        
        self.messages_processed = 0
        self.errors = 0
    
    def process_message(self, message):
        """Process viral content message"""
        start_time = time.time()
        
        try:
            data = json.loads(message.value().decode('utf-8'))
            trace_id = data.get('trace_id', 'unknown')
            content_id = data.get('content_id')
            views = data.get('views')
            engagement_rate = data.get('engagement_rate')
            
            with LogContext(self.logger, trace_id=trace_id,
                          content_id=content_id) as log:
                
                log.info("message_received",
                        partition=message.partition(),
                        offset=message.offset(),
                        views=views,
                        engagement_rate=engagement_rate,
                        start_time=data.get('timestamp'))
                
                # Validate engagement rate
                if engagement_rate < 0 or engagement_rate > 1:
                    raise ValueError(f"Invalid engagement rate: {engagement_rate}")
                
                # Calculate engagement score
                engagement_score = views * engagement_rate
                
                # Simulate cache update with occasional timeout
                if random.random() < 0.1:  # 10% chance of slow cache
                    time.sleep(0.5)  # Simulate timeout
                    log.warning("cache_slow_response",
                              latency_ms=500)
                
                if self.redis_client:
                    try:
                        self.redis_client.setex(
                            f"engagement:{content_id}",
                            3600,
                            str(engagement_score)
                        )
                        log.info("cache_updated",
                               engagement_score=round(engagement_score, 2))
                    except Exception as e:
                        log.error("cache_update_failed",
                                error=str(e),
                                error_type=type(e).__name__)
                
                processing_time = (time.time() - start_time) * 1000
                log.info("message_processed",
                        processing_time_ms=round(processing_time, 2),
                        engagement_score=round(engagement_score, 2))
                
                self.messages_processed += 1
                
        except Exception as e:
            self.errors += 1
            processing_time = (time.time() - start_time) * 1000
            
            self.logger.error("processing_error",
                            error=str(e),
                            error_type=type(e).__name__,
                            partition=message.partition(),
                            offset=message.offset(),
                            processing_time_ms=round(processing_time, 2))
    
    def consume(self, duration_seconds=60):
        """Consume messages for specified duration"""
        self.logger.info("consumption_started",
                        duration_seconds=duration_seconds)
        
        start = time.time()
        
        try:
            while time.time() - start < duration_seconds:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug("partition_eof",
                                        partition=msg.partition())
                    else:
                        self.logger.error("consumer_error",
                                        error=str(msg.error()))
                    continue
                
                self.process_message(msg)
                self.consumer.commit(message=msg)
                
        except KeyboardInterrupt:
            self.logger.info("consumption_interrupted")
        finally:
            elapsed = time.time() - start
            self.logger.info("consumption_completed",
                           duration_seconds=round(elapsed, 2),
                           messages_processed=self.messages_processed,
                           errors=self.errors,
                           success_rate=round((self.messages_processed / 
                                             (self.messages_processed + self.errors + 0.001)) * 100, 2))
    
    def close(self):
        """Close consumer"""
        self.consumer.close()
        if self.redis_client:
            self.redis_client.close()
        self.logger.info("consumer_closed")

if __name__ == '__main__':
    consumer = EngagementConsumer()
    try:
        consumer.consume(duration_seconds=60)
    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        consumer.close()
