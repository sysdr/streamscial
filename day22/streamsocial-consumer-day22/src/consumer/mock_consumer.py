#!/usr/bin/env python3
"""Mock consumer for demonstration purposes - generates fake metrics."""

import time
import random
import asyncio
import logging
from prometheus_client import Counter, Histogram, start_http_server

# Metrics
MESSAGES_PROCESSED = Counter('messages_processed_total', 'Total processed messages', ['topic', 'status'])
PROCESSING_LATENCY = Histogram('processing_latency_seconds', 'Message processing latency')
END_TO_END_LATENCY = Histogram('end_to_end_latency_seconds', 'End-to-end message latency')

class MockConsumer:
    """Mock consumer that generates fake metrics for demonstration."""
    
    def __init__(self):
        self.running = False
        self.stats = {
            'messages_processed': 0,
            'average_latency': 0,
            'max_latency': 0,
            'errors': 0
        }
        
    def start_metrics_server(self):
        """Start Prometheus metrics server."""
        try:
            start_http_server(8082)
            print("Metrics server started on port 8082")
        except OSError as e:
            print(f"Metrics server failed to start: {e}")
            print("Skipping metrics server (may already be running)")
    
    async def consume_loop(self):
        """Main consumption loop that generates fake metrics."""
        topics = ['critical-notifications', 'important-notifications', 'standard-notifications']
        print(f"Starting mock consumer for topics: {topics}")
        self.running = True
        
        message_count = 0
        
        while self.running:
            try:
                # Simulate processing 10 messages per second
                await asyncio.sleep(0.1)
                
                # Generate fake metrics
                topic = random.choice(topics)
                
                # Simulate latency (10-50ms)
                latency = random.uniform(0.01, 0.05)
                END_TO_END_LATENCY.observe(latency)
                PROCESSING_LATENCY.observe(latency * 0.5)
                
                # Update metrics
                MESSAGES_PROCESSED.labels(topic=topic, status='success').inc()
                
                # Update stats
                message_count += 1
                self.stats['messages_processed'] = message_count
                self.stats['average_latency'] = latency * 1000
                self.stats['max_latency'] = max(self.stats['max_latency'], latency * 1000)
                
                if message_count % 10 == 0:
                    print(f"Processed {message_count} messages (mock data)")
                
            except Exception as e:
                print(f"Error in mock consumer: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(1)
    
    def get_stats(self):
        """Get current consumer statistics."""
        return self.stats.copy()
    
    def stop(self):
        """Gracefully stop the consumer."""
        self.running = False
        print("Mock consumer stopped")

async def main():
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    consumer = MockConsumer()
    consumer.start_metrics_server()
    
    try:
        await consumer.consume_loop()
    except KeyboardInterrupt:
        print("Received interrupt signal")
    finally:
        consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
