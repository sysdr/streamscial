from kafka import KafkaConsumer
import json
import threading
import time
from collections import defaultdict
from typing import Set, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DuplicateDetector:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.bootstrap_servers = bootstrap_servers
        self.seen_posts: Set[str] = set()
        self.duplicates_found: Set[str] = set()
        self.post_counts: Dict[str, int] = defaultdict(int)
        self.running = False
        self.consumer = None
        
    def start_monitoring(self):
        """Start monitoring for duplicates in background thread"""
        self.running = True
        thread = threading.Thread(target=self._monitor_loop, daemon=True)
        thread.start()
        return thread
        
    def _monitor_loop(self):
        """Main monitoring loop"""
        try:
            self.consumer = KafkaConsumer(
                'streamsocial-posts',
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',
                group_id='duplicate-detector'
            )
            
            logger.info("Duplicate detector started monitoring...")
            
            for message in self.consumer:
                if not self.running:
                    break
                    
                post_data = message.value
                post_id = post_data.get('post_id')
                
                if post_id:
                    self.post_counts[post_id] += 1
                    
                    if post_id in self.seen_posts:
                        self.duplicates_found.add(post_id)
                        logger.warning(f"DUPLICATE DETECTED: {post_id}")
                    else:
                        self.seen_posts.add(post_id)
                        
        except Exception as e:
            logger.error(f"Error in duplicate detector: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                
    def get_stats(self) -> Dict[str, Any]:
        """Get duplicate detection statistics"""
        return {
            'total_posts': len(self.seen_posts),
            'duplicates_found': len(self.duplicates_found),
            'duplicate_rate': len(self.duplicates_found) / max(len(self.seen_posts), 1),
            'post_counts': dict(self.post_counts)
        }
        
    def stop(self):
        """Stop monitoring"""
        self.running = False
