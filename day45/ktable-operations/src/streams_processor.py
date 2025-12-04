import json
import time
import rocksdict
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict
from datetime import datetime
from src.models import UserAction, ReputationScore, ActionType
from src.reputation_calculator import ReputationCalculator
from config.settings import settings

class KTableReputationProcessor:
    """KTable-based reputation score processor with materialized views"""
    
    def __init__(self):
        # Consumer for user actions (KStream)
        self.consumer = KafkaConsumer(
            settings.user_actions_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.application_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        # Producer for changelog (KTable backing)
        self.changelog_producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        
        # RocksDB state store (materialized view)
        state_path = Path(settings.state_dir)
        state_path.mkdir(parents=True, exist_ok=True)
        
        self.state_store = rocksdict.Rdict(str(state_path / "reputation-store"))
        
        # Metrics
        self.metrics = {
            'events_processed': 0,
            'updates_written': 0,
            'unique_users': 0,
            'start_time': time.time()
        }
        
        self.calculator = ReputationCalculator()
        self.running = False
        
    def get_or_create_score(self, user_id: str) -> ReputationScore:
        """Get current score from state store or create new"""
        try:
            data = self.state_store.get(user_id)
            if data:
                score_dict = json.loads(data)
                return ReputationScore(**score_dict)
        except KeyError:
            pass
        
        # Create new score
        return ReputationScore(
            user_id=user_id,
            score=0,
            like_count=0,
            comment_count=0,
            share_count=0,
            last_updated=datetime.utcnow()
        )
    
    def update_state(self, user_id: str, score: ReputationScore):
        """Update materialized view and changelog"""
        # Write to RocksDB (local materialized view)
        self.state_store[user_id] = json.dumps(score.model_dump(mode='json'))
        
        # Write to changelog topic (for fault tolerance)
        self.changelog_producer.send(
            settings.reputation_changelog_topic,
            key=user_id,
            value=score.model_dump(mode='json')
        )
        
        self.metrics['updates_written'] += 1
    
    def process_stream(self):
        """Process user action stream and maintain KTable"""
        self.running = True
        print(f"Starting KTable processor: {settings.application_id}")
        print(f"State store: {settings.state_dir}")
        
        batch_size = 100
        batch_count = 0
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                user_id = message.key
                action_data = message.value
                
                # Parse action
                action = UserAction(**action_data)
                
                # Get current score (KTable lookup)
                current_score = self.get_or_create_score(user_id)
                
                # Update score (aggregation function)
                updated_score = self.calculator.update_score(
                    current_score,
                    action.action_type
                )
                
                # Update materialized view
                self.update_state(user_id, updated_score)
                
                self.metrics['events_processed'] += 1
                batch_count += 1
                
                # Periodic metrics
                if batch_count >= batch_size:
                    self.print_metrics()
                    batch_count = 0
                    
        except KeyboardInterrupt:
            print("\nStopping processor...")
        finally:
            self.cleanup()
    
    def print_metrics(self):
        """Print processing metrics"""
        elapsed = time.time() - self.metrics['start_time']
        rate = self.metrics['events_processed'] / elapsed if elapsed > 0 else 0
        
        # Count unique users in state store
        self.metrics['unique_users'] = len(self.state_store)
        
        print(f"Events: {self.metrics['events_processed']:,} | "
              f"Users: {self.metrics['unique_users']:,} | "
              f"Rate: {rate:.1f} events/sec | "
              f"Updates: {self.metrics['updates_written']:,}")
    
    def cleanup(self):
        """Clean shutdown"""
        print("Flushing state...")
        self.changelog_producer.flush()
        self.changelog_producer.close()
        self.consumer.close()
        self.state_store.close()
        self.print_metrics()

if __name__ == "__main__":
    processor = KTableReputationProcessor()
    processor.process_stream()

