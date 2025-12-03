"""Main Kafka Streams processor for content moderation pipeline."""

import json
import time
import signal
import sys
from typing import Optional
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID,
    TOPIC_RAW_POSTS, TOPIC_MODERATED, TOPIC_NOTIFICATIONS,
    BATCH_SIZE, PROCESSING_TIMEOUT_MS
)
from filters import spam_filter, policy_filter
from transformers import content_enricher, mention_extractor
from monitoring import metrics

class StreamProcessor:
    """Stateless transformations processor."""
    
    def __init__(self):
        self.running = True
        
        # Configure consumer
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000
        })
        
        # Configure producer
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'linger.ms': 10,
            'compression.type': 'snappy'
        })
        
        # Subscribe to input topic
        self.consumer.subscribe([TOPIC_RAW_POSTS])
        
        print(f"[INFO] Stream processor initialized")
        print(f"[INFO] Subscribed to: {TOPIC_RAW_POSTS}")
        print(f"[INFO] Output topics: {TOPIC_MODERATED}, {TOPIC_NOTIFICATIONS}")
    
    def process_record(self, record: dict) -> Optional[dict]:
        """
        Process a single record through the pipeline.
        Returns enriched record or None if filtered out.
        """
        # Peek: Record received
        metrics.record_received(record)
        
        # Filter: Spam detection
        is_spam, spam_reason = spam_filter.is_spam(record)
        metrics.record_spam_checked(record, is_spam, spam_reason)
        
        if is_spam:
            return None  # Filter out spam
        
        # Filter: Policy check
        policy_action, violations = policy_filter.check_policy(record)
        metrics.record_policy_checked(record, policy_action)
        
        if policy_action == 'REMOVE':
            return None  # Filter out policy violations
        
        # Map: Enrich content
        enriched = content_enricher.enrich_post(record)
        enriched['policy_action'] = policy_action
        enriched['policy_violations'] = violations
        
        # Peek: Record enrichment
        metrics.record_enriched(enriched)
        
        return enriched
    
    def process_mentions(self, record: dict):
        """
        Extract mentions and produce notification events.
        This demonstrates flatMap: one record -> many events.
        """
        events = mention_extractor.extract_events(record)
        
        for event in events:
            self.producer.produce(
                TOPIC_NOTIFICATIONS,
                key=event.get('target_user', '').encode('utf-8'),
                value=json.dumps(event).encode('utf-8')
            )
    
    def run(self):
        """Main processing loop."""
        print("[INFO] Starting processing loop...")
        
        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                try:
                    # Decode message
                    record = json.loads(msg.value().decode('utf-8'))
                    
                    # Process through pipeline
                    enriched = self.process_record(record)
                    
                    if enriched is not None:
                        # Produce enriched record
                        self.producer.produce(
                            TOPIC_MODERATED,
                            key=enriched.get('id', '').encode('utf-8'),
                            value=json.dumps(enriched).encode('utf-8')
                        )
                        
                        # Extract mentions (flatMap)
                        self.process_mentions(enriched)
                    
                    # Flush periodically
                    if self.consumer.position(self.consumer.assignment()) % BATCH_SIZE == 0:
                        self.producer.flush()
                
                except Exception as e:
                    print(f"[ERROR] Processing error: {e}")
                    continue
        
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        self.producer.flush()
        self.consumer.close()
        print("[INFO] Stream processor stopped")
    
    def handle_signal(self, signum, frame):
        """Handle shutdown signals."""
        print(f"\n[INFO] Received signal {signum}")
        self.running = False


def main():
    processor = StreamProcessor()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, processor.handle_signal)
    signal.signal(signal.SIGTERM, processor.handle_signal)
    
    # Run processor
    processor.run()


if __name__ == '__main__':
    main()
