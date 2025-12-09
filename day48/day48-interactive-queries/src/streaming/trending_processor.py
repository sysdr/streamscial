import json
import time
import re
import threading
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
from kafka import KafkaConsumer, KafkaProducer
from sortedcontainers import SortedDict
import rocksdict

class WindowStore:
    """Custom WindowStore implementation using RocksDB"""
    
    def __init__(self, name: str, window_size_ms: int, retention_ms: int):
        self.name = name
        self.window_size_ms = window_size_ms
        self.retention_ms = retention_ms
        self.store = rocksdict.Rdict(f"data/state-stores/{name}")
        
    def put(self, key: str, value: int, timestamp: int):
        """Store value in window"""
        window_start = self._get_window_start(timestamp)
        composite_key = f"{key}:{window_start}"
        self.store[composite_key] = str(value)
        
    def fetch(self, key: str, time_from: int, time_to: int) -> List[Tuple[int, int]]:
        """Fetch all windows for key in time range"""
        results = []
        current = self._get_window_start(time_from)
        
        while current <= time_to:
            composite_key = f"{key}:{current}"
            if composite_key in self.store:
                value = int(self.store[composite_key])
                results.append((current, value))
            current += self.window_size_ms
            
        return results
    
    def fetch_all(self, time_from: int, time_to: int) -> Dict[str, List[Tuple[int, int]]]:
        """Fetch all keys in time range"""
        results = defaultdict(list)
        
        for key in self.store.keys():
            # Handle both bytes and string keys
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            parts = key.rsplit(':', 1)
            if len(parts) == 2:
                hashtag, window = parts[0], int(parts[1])
                if time_from <= window <= time_to:
                    value = int(self.store[key])
                    results[hashtag].append((window, value))
        
        return dict(results)
    
    def _get_window_start(self, timestamp: int) -> int:
        """Calculate window start time"""
        return (timestamp // self.window_size_ms) * self.window_size_ms
    
    def close(self):
        """Close store"""
        self.store.close()

class TrendingProcessor:
    """Kafka Streams processor for trending topics"""
    
    def __init__(self, application_id: str, bootstrap_servers: str, 
                 instance_id: int, port: int):
        self.application_id = application_id
        self.bootstrap_servers = bootstrap_servers
        self.instance_id = instance_id
        self.port = port
        
        # Window configuration: 5-minute windows
        self.window_size_ms = 5 * 60 * 1000
        self.retention_ms = 60 * 60 * 1000  # 1 hour retention
        
        # Create window store
        self.window_store = WindowStore(
            f"hashtag-trends-{instance_id}",
            self.window_size_ms,
            self.retention_ms
        )
        
        # Metadata
        self.metadata = {
            'instance_id': instance_id,
            'host': 'localhost',
            'port': port,
            'state': 'RUNNING'
        }
        
        self.running = False
        
    def extract_hashtags(self, text: str) -> List[str]:
        """Extract hashtags from post text"""
        return re.findall(r'#(\w+)', text)
    
    def process(self):
        """Main processing loop"""
        consumer = KafkaConsumer(
            'social.posts',
            bootstrap_servers=self.bootstrap_servers,
            group_id=f'{self.application_id}-{self.instance_id}',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        
        print(f"[Instance {self.instance_id}] Started processing on port {self.port}")
        self.running = True
        
        # Window counts
        window_counts = defaultdict(lambda: defaultdict(int))
        
        try:
            for message in consumer:
                if not self.running:
                    break
                    
                post = message.value
                timestamp = int(post.get('timestamp', time.time() * 1000))
                text = post.get('text', '')
                
                # Extract and count hashtags
                hashtags = self.extract_hashtags(text)
                window_start = self._get_window_start(timestamp)
                
                for hashtag in hashtags:
                    hashtag = hashtag.lower()
                    window_counts[window_start][hashtag] += 1
                    
                    # Update window store
                    current_count = window_counts[window_start][hashtag]
                    self.window_store.put(hashtag, current_count, timestamp)
                
                # Publish to changelog for state replication
                producer.send('trending-changelog', {
                    'hashtags': hashtags,
                    'timestamp': timestamp,
                    'window_start': window_start
                })
                
                # Cleanup old windows
                self._cleanup_old_windows(window_counts, timestamp)
                
        except KeyboardInterrupt:
            print(f"\n[Instance {self.instance_id}] Shutting down...")
        finally:
            consumer.close()
            producer.close()
            self.window_store.close()
            self.running = False
    
    def _get_window_start(self, timestamp: int) -> int:
        """Calculate window start"""
        return (timestamp // self.window_size_ms) * self.window_size_ms
    
    def _cleanup_old_windows(self, window_counts: dict, current_time: int):
        """Remove windows outside retention period"""
        cutoff = current_time - self.retention_ms
        windows_to_remove = [w for w in window_counts.keys() if w < cutoff]
        for window in windows_to_remove:
            del window_counts[window]
    
    def get_store(self) -> WindowStore:
        """Get window store for queries"""
        return self.window_store
    
    def get_metadata(self) -> dict:
        """Get instance metadata"""
        return self.metadata
    
    def stop(self):
        """Stop processor"""
        self.running = False
