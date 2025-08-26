import json
import time
import random
import threading
from typing import Dict, Any
from kafka import KafkaProducer
import logging

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

logger = logging.getLogger(__name__)

class InteractionProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            batch_size=16384,
            linger_ms=10,
            compression_type='gzip'
        )
        self.running = False
        
    def start_generating(self, events_per_second: int = 100):
        """Start generating user interaction events"""
        self.running = True
        self.events_per_second = events_per_second
        
        generator_thread = threading.Thread(target=self._generate_events, daemon=True)
        generator_thread.start()
        logger.info(f"Started generating {events_per_second} events per second")
        
    def _generate_events(self):
        """Generate continuous stream of user interactions"""
        while self.running:
            start_time = time.time()
            
            # Generate batch of events
            for _ in range(self.events_per_second):
                event = self._create_interaction_event()
                
                try:
                    self.producer.send(
                        KAFKA_TOPICS['user_interactions'],
                        key=event['user_id'],
                        value=event,
                        partition=hash(event['user_id']) % 6  # Distribute across 6 partitions
                    )
                except Exception as e:
                    logger.error(f"Error sending event: {e}")
                    
            # Sleep to maintain target rate
            elapsed = time.time() - start_time
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)
            
    def _create_interaction_event(self) -> Dict[str, Any]:
        """Create realistic user interaction event"""
        user_id = f"user_{random.randint(1, 100000)}"
        action_types = ['like', 'comment', 'share', 'view', 'follow', 'bookmark']
        content_types = ['post', 'story', 'reel', 'live', 'poll']
        
        return {
            'user_id': user_id,
            'action_type': random.choice(action_types),
            'content_id': f"content_{random.randint(1, 50000)}",
            'content_type': random.choice(content_types),
            'timestamp': time.time(),
            'metadata': {
                'platform': random.choice(['mobile', 'web', 'tablet']),
                'duration_ms': random.randint(100, 10000),
                'location': random.choice(['US', 'EU', 'ASIA', 'OTHER'])
            }
        }
        
    def create_traffic_spike(self, duration_seconds: int = 60, spike_multiplier: int = 10):
        """Create traffic spike for testing auto-scaling"""
        original_rate = self.events_per_second
        spike_rate = original_rate * spike_multiplier
        
        logger.info(f"Creating traffic spike: {original_rate} -> {spike_rate} events/sec for {duration_seconds}s")
        
        self.events_per_second = spike_rate
        time.sleep(duration_seconds)
        self.events_per_second = original_rate
        
        logger.info(f"Traffic spike ended: back to {original_rate} events/sec")
        
    def stop(self):
        """Stop event generation"""
        self.running = False
        self.producer.flush()
        self.producer.close()
        logger.info("Event producer stopped")
