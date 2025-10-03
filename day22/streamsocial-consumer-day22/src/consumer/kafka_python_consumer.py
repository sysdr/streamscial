#!/usr/bin/env python3
"""Working Kafka consumer using kafka-python library."""

import json
import time
import logging
import asyncio
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

# Metrics
MESSAGES_PROCESSED = Counter('messages_processed_total', 'Total processed messages', ['topic', 'status'])
PROCESSING_LATENCY = Histogram('processing_latency_seconds', 'Message processing latency')
END_TO_END_LATENCY = Histogram('end_to_end_latency_seconds', 'End-to-end message latency')

class KafkaPythonConsumer:
    """Working Kafka consumer using kafka-python library."""
    
    def __init__(self):
        # Create consumer with kafka-python
        self.consumer = KafkaConsumer(
            'critical-notifications',
            'important-notifications', 
            'standard-notifications',
            bootstrap_servers=['kafka:9092'],
            group_id='streamsocial-kafka-python-consumer',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        self.running = False
        self.stats = {
            'messages_processed': 0,
            'average_latency': 0,
            'max_latency': 0,
            'errors': 0
        }
        
        print("Kafka consumer created successfully with kafka-python")
        
    def start_metrics_server(self):
        """Start Prometheus metrics server."""
        try:
            start_http_server(8080)
            print("Metrics server started on port 8080")
        except OSError as e:
            print(f"Metrics server failed to start: {e}")
            print("Skipping metrics server (may already be running)")
    
    async def consume_loop(self):
        """Main consumption loop."""
        print("Starting kafka-python consumer loop...")
        self.running = True
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                print(f"Processing message from topic: {message.topic}")
                await self._process_message(message)
                
                # Commit message
                self.consumer.commit()
                
        except Exception as e:
            print(f"Consumer error: {e}")
            self.stats['errors'] += 1
    
    async def _process_message(self, message):
        """Process individual message."""
        try:
            # Get message data
            message_data = message.value
            message_data['topic'] = message.topic
            
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
            MESSAGES_PROCESSED.labels(topic=message.topic, status='success').inc()
            
            print(f"Processed message {self.stats['messages_processed']}: {message_data.get('id', 'unknown')} from {message.topic}")
            
        except Exception as e:
            print(f"Message processing error: {e}")
            self.stats['errors'] += 1
            MESSAGES_PROCESSED.labels(topic=message.topic, status='error').inc()
    
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
    
    consumer = KafkaPythonConsumer()
    consumer.start_metrics_server()
    
    try:
        await consumer.consume_loop()
    except KeyboardInterrupt:
        print("Received interrupt signal")
    finally:
        consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
