"""StreamSocial Low-Latency Notification Consumer."""

import asyncio
import json
import time
import logging
from typing import Dict, Any, List
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, start_http_server
import uvloop

from config.consumer_config import ConsumerConfig

# Metrics
MESSAGES_PROCESSED = Counter('messages_processed_total', 'Total processed messages', ['topic', 'status'])
PROCESSING_LATENCY = Histogram('processing_latency_seconds', 'Message processing latency')
END_TO_END_LATENCY = Histogram('end_to_end_latency_seconds', 'End-to-end message latency')

class NotificationProcessor:
    """Handles different types of notifications with optimized processing."""
    
    def __init__(self):
        self.processing_times = {
            'critical': 0.005,    # 5ms
            'important': 0.015,   # 15ms  
            'standard': 0.025     # 25ms
        }
    
    async def process_notification(self, message: Dict[str, Any]) -> bool:
        """Process notification with type-specific handling."""
        notification_type = message.get('type', 'standard')
        start_time = time.time()
        
        try:
            # Simulate processing based on notification type
            await asyncio.sleep(self.processing_times.get(notification_type, 0.025))
            
            # Mock delivery (replace with actual delivery logic)
            success = await self._deliver_notification(message)
            
            processing_time = time.time() - start_time
            PROCESSING_LATENCY.observe(processing_time)
            
            status = 'success' if success else 'failed'
            MESSAGES_PROCESSED.labels(topic=message.get('topic'), status=status).inc()
            
            return success
            
        except Exception as e:
            logging.error(f"Processing failed: {e}")
            MESSAGES_PROCESSED.labels(topic=message.get('topic'), status='error').inc()
            return False
    
    async def _deliver_notification(self, message: Dict[str, Any]) -> bool:
        """Mock notification delivery."""
        # In production: push to mobile, WebSocket, email, etc.
        notification_id = message.get('id', 'unknown')
        user_id = message.get('user_id', 'unknown')
        
        # Simulate delivery latency
        await asyncio.sleep(0.002)  # 2ms network call
        
        logging.info(f"Delivered notification {notification_id} to user {user_id}")
        return True

class LowLatencyConsumer:
    """Ultra-fast Kafka consumer for StreamSocial notifications."""
    
    def __init__(self):
        import os
        # Read environment variable directly to ensure it's current
        kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9092')
        print(f"Environment KAFKA_SERVERS: {kafka_servers}")
        
        # Create a completely fresh configuration with explicit settings
        config = {
            'bootstrap.servers': kafka_servers,
            'group.id': 'streamsocial-critical-notifications',
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',  # Start from beginning to catch existing messages
            'fetch.min.bytes': 1,
            'session.timeout.ms': 10000,  # Increase timeout
            'heartbeat.interval.ms': 3000,  # Increase heartbeat
            'max.poll.interval.ms': 300000,  # Increase poll interval
            'request.timeout.ms': 30000,  # Add request timeout
            'metadata.max.age.ms': 300000,  # Add metadata max age
        }
        print(f"Creating consumer with fresh config: {config}")
        
        self.consumer = Consumer(config)
        self.processor = NotificationProcessor()
        self.running = False
        self.stats = {
            'messages_processed': 0,
            'average_latency': 0,
            'max_latency': 0,
            'errors': 0
        }
        
    def start_metrics_server(self):
        """Start Prometheus metrics server."""
        start_http_server(ConsumerConfig.METRICS_PORT)
        logging.info(f"Metrics server started on port {ConsumerConfig.METRICS_PORT}")
    
    async def consume_loop(self):
        """Main consumption loop optimized for low latency."""
        print(f"Subscribing to topics: {ConsumerConfig.TOPICS}")
        self.consumer.subscribe(ConsumerConfig.TOPICS)
        self.running = True
        
        logging.info("Starting low-latency consumer...")
        print("Consumer loop started, beginning message polling...")
        
        while self.running:
            try:
                # Ultra-short poll timeout for minimal latency
                msg = self.consumer.poll(timeout=1.0)  # 1 second poll for debugging
                
                if msg is None:
                    print("No message received (timeout)")
                    await asyncio.sleep(0.1)  # Yield control briefly
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                        self.stats['errors'] += 1
                        continue
                
                # Process message
                print(f"Processing message from topic: {msg.topic()}")
                await self._process_message(msg)
                
                # Manual commit for better control
                self.consumer.commit(asynchronous=False)
                
            except Exception as e:
                logging.error(f"Consumption error: {e}")
                self.stats['errors'] += 1
    
    async def _process_message(self, msg):
        """Process individual message with latency tracking."""
        try:
            # Parse message
            message_data = json.loads(msg.value().decode('utf-8'))
            message_data['topic'] = msg.topic()
            
            # Calculate end-to-end latency
            message_timestamp = message_data.get('timestamp', time.time())
            current_time = time.time()
            e2e_latency = current_time - message_timestamp
            
            END_TO_END_LATENCY.observe(e2e_latency)
            
            # Process notification
            success = await self.processor.process_notification(message_data)
            
            # Update stats
            self.stats['messages_processed'] += 1
            self.stats['average_latency'] = (
                self.stats['average_latency'] * (self.stats['messages_processed'] - 1) + 
                e2e_latency * 1000
            ) / self.stats['messages_processed']
            
            if e2e_latency * 1000 > self.stats['max_latency']:
                self.stats['max_latency'] = e2e_latency * 1000
                
            if not success:
                self.stats['errors'] += 1
                
        except Exception as e:
            logging.error(f"Message processing error: {e}")
            self.stats['errors'] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current consumer statistics."""
        return self.stats.copy()
    
    def stop(self):
        """Gracefully stop the consumer."""
        self.running = False
        self.consumer.close()
        logging.info("Consumer stopped")

async def main():
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Use uvloop for better performance
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    
    consumer = LowLatencyConsumer()
    consumer.start_metrics_server()
    
    try:
        await consumer.consume_loop()
    except KeyboardInterrupt:
        logging.info("Received interrupt signal")
    finally:
        consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
