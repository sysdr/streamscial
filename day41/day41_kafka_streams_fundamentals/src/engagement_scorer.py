"""
Real-time Engagement Scoring Engine using Kafka Streams pattern
Processes user interactions to calculate real-time engagement scores
"""

import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from confluent_kafka import Consumer, Producer, KafkaError
import rocksdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StateStore:
    """Local state store using RocksDB for fault-tolerant state management"""
    
    def __init__(self, store_path: str):
        self.db = rocksdict.Rdict(store_path)
        self.changelog_producer = None
    
    def put(self, key: str, value: dict):
        """Store key-value pair and write to changelog"""
        self.db[key] = json.dumps(value)
        if self.changelog_producer:
            self.changelog_producer.produce(
                'engagement-scores-changelog',
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8')
            )
            self.changelog_producer.poll(0)
    
    def get(self, key: str) -> Optional[dict]:
        """Retrieve value from state store"""
        value = self.db.get(key)
        return json.loads(value) if value else None
    
    def scan(self, limit: int = 100) -> List[tuple]:
        """Scan state store for top scores"""
        items = []
        for key, value in self.db.items():
            items.append((key.decode() if isinstance(key, bytes) else key, 
                         json.loads(value)))
        return sorted(items, key=lambda x: x[1].get('score', 0), reverse=True)[:limit]
    
    def close(self):
        """Close state store"""
        self.db.close()


class WindowedAggregator:
    """Tumbling window aggregation for time-based scoring"""
    
    def __init__(self, window_size_seconds: int = 300):
        self.window_size = window_size_seconds
        self.windows = defaultdict(lambda: defaultdict(int))
    
    def add_event(self, content_id: str, weight: int, timestamp: float):
        """Add event to appropriate window"""
        window_start = int(timestamp // self.window_size) * self.window_size
        self.windows[content_id][window_start] += weight
    
    def get_score(self, content_id: str, current_time: float) -> int:
        """Calculate score from recent windows only"""
        cutoff_time = current_time - (self.window_size * 3)  # Keep last 3 windows
        total_score = 0
        
        if content_id in self.windows:
            for window_start, score in list(self.windows[content_id].items()):
                if window_start >= cutoff_time:
                    total_score += score
                else:
                    # Expire old windows
                    del self.windows[content_id][window_start]
        
        return total_score
    
    def cleanup(self, current_time: float):
        """Remove expired windows"""
        cutoff_time = current_time - (self.window_size * 3)
        
        for content_id in list(self.windows.keys()):
            for window_start in list(self.windows[content_id].keys()):
                if window_start < cutoff_time:
                    del self.windows[content_id][window_start]
            
            if not self.windows[content_id]:
                del self.windows[content_id]


class EngagementScoringTopology:
    """Kafka Streams topology for real-time engagement scoring"""
    
    INTERACTION_WEIGHTS = {
        'VIEW': 0.1,
        'LIKE': 1,
        'COMMENT': 5,
        'SHARE': 10
    }
    
    def __init__(self, bootstrap_servers: str, group_id: str, state_dir: str):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        
        # Initialize state store
        self.state_store = StateStore(f"{state_dir}/engagement-scores")
        
        # Initialize windowed aggregator
        self.aggregator = WindowedAggregator(window_size_seconds=300)
        
        # Kafka consumer for input stream
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed'
        })
        
        # Kafka producer for output stream and changelog
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'enable.idempotence': True,
            'acks': 'all',
            'transactional.id': f'{group_id}-txn'
        })
        
        self.producer.init_transactions()
        self.state_store.changelog_producer = self.producer
        
        self.running = False
        self.metrics = {
            'processed': 0,
            'filtered': 0,
            'aggregated': 0,
            'errors': 0
        }
    
    def filter_processor(self, event: dict) -> bool:
        """Filter stage: remove invalid or spam interactions"""
        if not event.get('content_id') or not event.get('interaction_type'):
            self.metrics['filtered'] += 1
            return False
        
        # Simple spam detection: ignore if user_id is missing
        if not event.get('user_id'):
            self.metrics['filtered'] += 1
            return False
        
        return True
    
    def map_processor(self, event: dict) -> dict:
        """Map stage: enrich events with weights"""
        interaction_type = event['interaction_type'].upper()
        weight = self.INTERACTION_WEIGHTS.get(interaction_type, 0)
        
        return {
            'content_id': event['content_id'],
            'user_id': event['user_id'],
            'interaction_type': interaction_type,
            'weight': weight,
            'timestamp': event.get('timestamp', time.time())
        }
    
    def aggregate_processor(self, event: dict):
        """Aggregate stage: update windowed scores"""
        content_id = event['content_id']
        weight = event['weight']
        timestamp = event['timestamp']
        
        # Add to windowed aggregator
        self.aggregator.add_event(content_id, weight, timestamp)
        
        # Calculate current score
        current_score = self.aggregator.get_score(content_id, time.time())
        
        # Update state store
        score_data = {
            'content_id': content_id,
            'score': current_score,
            'last_updated': time.time(),
            'window_size': 300
        }
        
        self.state_store.put(content_id, score_data)
        self.metrics['aggregated'] += 1
        
        return score_data
    
    def sink_processor(self, score_data: dict):
        """Sink stage: write to output topic"""
        self.producer.produce(
            'engagement-scores',
            key=score_data['content_id'].encode('utf-8'),
            value=json.dumps(score_data).encode('utf-8')
        )
    
    def process_record(self, record):
        """Process single record through topology"""
        try:
            # Source: parse input event
            event = json.loads(record.value().decode('utf-8'))
            
            # Filter
            if not self.filter_processor(event):
                return
            
            # Map
            enriched_event = self.map_processor(event)
            
            # Aggregate
            score_data = self.aggregate_processor(enriched_event)
            
            # Sink
            self.sink_processor(score_data)
            
            self.metrics['processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            self.metrics['errors'] += 1
    
    def start(self):
        """Start stream processing"""
        self.running = True
        self.consumer.subscribe(['user-interactions'])
        
        logger.info("Starting engagement scoring topology...")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Begin transaction for exactly-once semantics
                self.producer.begin_transaction()
                
                try:
                    # Process record
                    self.process_record(msg)
                    
                    # Commit consumer offset within transaction
                    self.producer.send_offsets_to_transaction(
                        self.consumer.position(self.consumer.assignment()),
                        self.consumer.consumer_group_metadata()
                    )
                    
                    # Commit transaction
                    self.producer.commit_transaction()
                    
                except Exception as e:
                    logger.error(f"Transaction error: {e}")
                    self.producer.abort_transaction()
                
                # Periodic cleanup
                if self.metrics['processed'] % 1000 == 0:
                    self.aggregator.cleanup(time.time())
                    logger.info(f"Metrics: {self.metrics}")
        
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        
        finally:
            self.stop()
    
    def stop(self):
        """Graceful shutdown"""
        self.running = False
        self.consumer.close()
        self.producer.flush()
        self.state_store.close()
        logger.info("Topology stopped")
    
    def query_state(self, content_id: str) -> Optional[dict]:
        """Interactive query: read from local state store"""
        return self.state_store.get(content_id)
    
    def query_top_scores(self, limit: int = 10) -> List[dict]:
        """Interactive query: get top scoring content"""
        items = self.state_store.scan(limit)
        return [{'content_id': k, **v} for k, v in items]


if __name__ == '__main__':
    topology = EngagementScoringTopology(
        bootstrap_servers='localhost:9092',
        group_id='engagement-scorer-v1',
        state_dir='/tmp/kafka-streams-state'
    )
    
    topology.start()
