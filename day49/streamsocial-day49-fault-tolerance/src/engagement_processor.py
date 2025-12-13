import json
import time
import threading
import signal
import sys
from typing import Dict, Optional
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from rocksdict import Rdict
import math
import os

class EngagementScoreProcessor:
    """Stateful processor with automatic changelog-backed recovery"""
    
    def __init__(self, bootstrap_servers: str, group_id: str, state_dir: str):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.state_dir = state_dir
        self.running = False
        
        # State store (RocksDB)
        os.makedirs(state_dir, exist_ok=True)
        self.state_store = Rdict(f"{state_dir}/engagement_scores.db")
        
        # Changelog topic for state backup
        self.changelog_topic = f"{group_id}-engagement-scores-changelog"
        
        # Metrics
        self.metrics = {
            'events_processed': 0,
            'state_updates': 0,
            'recovery_time_ms': 0,
            'is_restoring': False,
            'last_checkpoint': time.time()
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._graceful_shutdown)
        signal.signal(signal.SIGTERM, self._graceful_shutdown)
        
    def _graceful_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        print(f"\n[SHUTDOWN] Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def _create_consumer(self) -> Consumer:
        """Create Kafka consumer with exactly-once config"""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
            'max.poll.interval.ms': 300000
        }
        return Consumer(config)
        
    def _create_producer(self) -> Producer:
        """Create Kafka producer for changelog"""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': 'all',
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 5,
            'compression.type': 'snappy'
        }
        return Producer(config)
        
    def restore_state_from_changelog(self):
        """Restore state store from changelog topic"""
        print(f"[RESTORE] Starting state restoration from {self.changelog_topic}")
        self.metrics['is_restoring'] = True
        start_time = time.time()
        
        consumer = self._create_consumer()
        consumer.subscribe([self.changelog_topic])
        
        restored_count = 0
        timeout = 10.0
        
        try:
            while True:
                msg = consumer.poll(timeout)
                
                if msg is None:
                    break
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    raise KafkaException(msg.error())
                
                # Restore state from changelog
                key_data = msg.key()
                if isinstance(key_data, bytes):
                    key = key_data.decode('utf-8')
                else:
                    key = key_data
                
                value_data = msg.value()
                if isinstance(value_data, bytes):
                    value = json.loads(value_data.decode('utf-8'))
                else:
                    value = json.loads(value_data) if isinstance(value_data, str) else value_data
                
                self.state_store[key] = json.dumps(value)
                restored_count += 1
                
        finally:
            consumer.close()
            
        self.metrics['recovery_time_ms'] = int((time.time() - start_time) * 1000)
        self.metrics['is_restoring'] = False
        print(f"[RESTORE] Completed: {restored_count} states restored in {self.metrics['recovery_time_ms']}ms")
        
    def calculate_engagement_score(self, user_id: str, event_type: str, timestamp: float) -> float:
        """Calculate engagement score with time decay"""
        # Get current state
        state_data = self.state_store.get(user_id, b'null')
        if isinstance(state_data, bytes):
            state_json = state_data.decode('utf-8')
        else:
            state_json = state_data if state_data != 'null' else 'null'
        
        state = json.loads(state_json) if state_json != 'null' else {
            'likes': 0, 'comments': 0, 'shares': 0, 'last_update': timestamp
        }
        
        # Update interaction counts
        if event_type == 'like':
            state['likes'] += 1
        elif event_type == 'comment':
            state['comments'] += 1
        elif event_type == 'share':
            state['shares'] += 1
            
        # Calculate base score
        base_score = (state['likes'] * 1.0) + (state['comments'] * 2.0) + (state['shares'] * 3.0)
        
        # Apply time decay (24-hour half-life)
        hours_elapsed = (timestamp - state['last_update']) / 3600.0
        decay_factor = math.exp(-hours_elapsed / 24.0)
        final_score = base_score * decay_factor
        
        state['last_update'] = timestamp
        state['score'] = round(final_score, 2)
        
        return state
        
    def write_to_changelog(self, producer: Producer, user_id: str, state: Dict):
        """Write state update to changelog topic"""
        try:
            producer.produce(
                self.changelog_topic,
                key=user_id.encode('utf-8'),
                value=json.dumps(state).encode('utf-8')
            )
            producer.poll(0)  # Trigger callbacks
        except Exception as e:
            print(f"[ERROR] Changelog write failed: {e}")
            
    def process_events(self):
        """Main event processing loop"""
        self.running = True
        
        # Restore state if needed
        try:
            self.restore_state_from_changelog()
        except Exception as e:
            print(f"[RESTORE] No changelog found or restoration failed: {e}")
            
        consumer = self._create_consumer()
        producer = self._create_producer()
        
        consumer.subscribe(['user-interactions'])
        
        print("[PROCESSOR] Starting event processing...")
        
        try:
            while self.running:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                
                # Process event
                event = json.loads(msg.value().decode('utf-8'))
                user_id = event['user_id']
                event_type = event['type']
                timestamp = event.get('timestamp', time.time())
                
                # Calculate new engagement score
                new_state = self.calculate_engagement_score(user_id, event_type, timestamp)
                
                # Update state store
                self.state_store[user_id] = json.dumps(new_state)
                
                # Write to changelog
                self.write_to_changelog(producer, user_id, new_state)
                
                self.metrics['events_processed'] += 1
                self.metrics['state_updates'] += 1
                
                if self.metrics['events_processed'] % 100 == 0:
                    print(f"[STATS] Processed: {self.metrics['events_processed']} events")
                
                # Commit offsets periodically
                if time.time() - self.metrics['last_checkpoint'] > 5.0:
                    consumer.commit(asynchronous=False)
                    producer.flush()
                    self.metrics['last_checkpoint'] = time.time()
                    
        except KeyboardInterrupt:
            print("\n[PROCESSOR] Interrupted by user")
        finally:
            print("[PROCESSOR] Cleaning up...")
            consumer.commit()
            producer.flush()
            consumer.close()
            self.state_store.close()
            
    def get_metrics(self) -> Dict:
        """Return current metrics"""
        return self.metrics.copy()
        
    def get_top_users(self, limit: int = 10) -> list:
        """Get top users by engagement score"""
        scores = []
        for key in self.state_store.keys():
            # Handle both bytes and string keys
            if isinstance(key, bytes):
                user_id = key.decode('utf-8')
            else:
                user_id = key
            
            # Handle both bytes and string values
            state_data = self.state_store[key]
            if isinstance(state_data, bytes):
                state = json.loads(state_data.decode('utf-8'))
            else:
                state = json.loads(state_data) if isinstance(state_data, str) else state_data
            
            scores.append({
                'user_id': user_id,
                'score': state.get('score', 0),
                'likes': state.get('likes', 0),
                'comments': state.get('comments', 0),
                'shares': state.get('shares', 0)
            })
        
        scores.sort(key=lambda x: x['score'], reverse=True)
        return scores[:limit]

if __name__ == '__main__':
    processor = EngagementScoreProcessor(
        bootstrap_servers='localhost:9092',
        group_id='engagement-processor',
        state_dir='/tmp/kafka-streams-state'
    )
    processor.process_events()
