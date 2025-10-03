#!/usr/bin/env python3
"""Simple working consumer for StreamSocial notifications."""

import os
import json
import time
import logging
import asyncio
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Histogram, start_http_server

# Set environment variables explicitly
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'kafka:9092'
os.environ['KAFKA_SERVERS'] = 'kafka:9092'

# Metrics
MESSAGES_PROCESSED = Counter('messages_processed_total', 'Total processed messages', ['topic', 'status'])
PROCESSING_LATENCY = Histogram('processing_latency_seconds', 'Message processing latency')
END_TO_END_LATENCY = Histogram('end_to_end_latency_seconds', 'End-to-end message latency')

class SimpleConsumer:
    """Simple working Kafka consumer for StreamSocial notifications."""
    
    def __init__(self):
        # Create consumer with explicit configuration
        config = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'streamsocial-simple-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 10000,
            'heartbeat.interval.ms': 3000,
        }
        
        print(f"Creating simple consumer with config: {config}")
        self.consumer = Consumer(config)
        self.running = False
        self.stats = {
            'messages_processed': 0,
            'average_latency': 0,
            'max_latency': 0,
            'errors': 0
        }
        
    def start_metrics_server(self):
        """Start Prometheus metrics server."""
        start_http_server(8081)
        print("Metrics server started on port 8081")
    
    async def consume_loop(self):
        """Main consumption loop."""
        topics = ['critical-notifications', 'important-notifications', 'standard-notifications']
        print(f"Subscribing to topics: {topics}")
        self.consumer.subscribe(topics)
        self.running = True
        
        print("Starting simple consumer loop...")
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    print("No message received (timeout)")
                    await asyncio.sleep(0.1)
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("End of partition reached")
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        self.stats['errors'] += 1
                        continue
                
                # Process message
                print(f"Processing message from topic: {msg.topic()}")
                await self._process_message(msg)
                
                # Commit message
                self.consumer.commit(asynchronous=False)
                
            except Exception as e:
                print(f"Consumption error: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(1)
    
    async def _process_message(self, msg):
        """Process individual message."""
        try:
            # Parse message
            message_data = json.loads(msg.value().decode('utf-8'))
            message_data['topic'] = msg.topic()
            
            # Calculate end-to-end latency
            message_timestamp = message_data.get('timestamp', time.time())
            current_time = time.time()
            e2e_latency = current_time - message_timestamp
            
            END_TO_END_LATENCY.observe(e2e_latency)
            
            # Simulate processing
            processing_start = time.time()
            await asyncio.sleep(0.01)  # 10ms processing
            processing_time = time.time() - processing_start
            
            PROCESSING_LATENCY.observe(processing_time)
            
            # Update stats
            self.stats['messages_processed'] += 1
            self.stats['average_latency'] = (
                self.stats['average_latency'] * (self.stats['messages_processed'] - 1) + 
                e2e_latency * 1000
            ) / self.stats['messages_processed']
            
            if e2e_latency * 1000 > self.stats['max_latency']:
                self.stats['max_latency'] = e2e_latency * 1000
            
            # Update metrics
            MESSAGES_PROCESSED.labels(topic=msg.topic(), status='success').inc()
            
            print(f"Processed message {self.stats['messages_processed']}: {message_data.get('id', 'unknown')} from {msg.topic()}")
            
        except Exception as e:
            print(f"Message processing error: {e}")
            self.stats['errors'] += 1
            MESSAGES_PROCESSED.labels(topic=msg.topic(), status='error').inc()
    
    def get_stats(self):
        """Get current consumer statistics."""
        return self.stats.copy()
    
    def stop(self):
        """Gracefully stop the consumer."""
        self.running = False
        self.consumer.close()
        print("Consumer stopped")

async def main():
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    consumer = SimpleConsumer()
    consumer.start_metrics_server()
    
    try:
        await consumer.consume_loop()
    except KeyboardInterrupt:
        print("Received interrupt signal")
    finally:
        consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
