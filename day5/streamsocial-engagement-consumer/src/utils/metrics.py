import time
import psutil
import redis
import json
from prometheus_client import Counter, Histogram, Gauge
from typing import Dict, Any
from config.consumer_config import ConsumerConfig

# Prometheus metrics
MESSAGES_PROCESSED = Counter('kafka_messages_processed_total', 'Total processed messages', ['status'])
PROCESSING_TIME = Histogram('kafka_message_processing_seconds', 'Message processing time')
CONSUMER_LAG = Gauge('kafka_consumer_lag', 'Consumer lag', ['topic', 'partition'])
ENGAGEMENT_EVENTS = Counter('engagement_events_total', 'Total engagement events', ['action_type'])

class MetricsCollector:
    def __init__(self):
        self.start_time = time.time()
        self.redis_client = redis.Redis(
            host=ConsumerConfig.REDIS_HOST,
            port=ConsumerConfig.REDIS_PORT,
            db=ConsumerConfig.REDIS_DB,
            decode_responses=True
        )
        self.processed_count = 0
        self.error_count = 0
        self.processing_times = []
    
    def record_message_processed(self, processing_time: float, success: bool):
        """Record message processing metrics"""
        self.processed_count += 1
        if not success:
            self.error_count += 1
            
        # Store metrics in Redis for persistence across processes
        metrics_data = {
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'processing_times': self.processing_times[-100:] if self.processing_times else []  # Keep last 100
        }
        
        try:
            self.redis_client.set('metrics:message_processing', json.dumps(metrics_data), ex=3600)  # 1 hour expiry
        except Exception:
            pass  # Ignore Redis errors, fall back to local storage
            
        MESSAGES_PROCESSED.labels(status='success' if success else 'error').inc()
        PROCESSING_TIME.observe(processing_time)
        self.processing_times.append(processing_time)
        
        # Keep only last 1000 processing times
        if len(self.processing_times) > 1000:
            self.processing_times = self.processing_times[-1000:]
    
    def record_engagement_event(self, action_type: str):
        """Record engagement event metrics"""
        ENGAGEMENT_EVENTS.labels(action_type=action_type).inc()
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics"""
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        
        # Try to get metrics from Redis first
        try:
            redis_metrics = self.redis_client.get('metrics:message_processing')
            if redis_metrics:
                metrics_data = json.loads(redis_metrics)
                self.processed_count = metrics_data.get('processed_count', 0)
                self.error_count = metrics_data.get('error_count', 0)
                self.processing_times = metrics_data.get('processing_times', [])
        except Exception:
            pass  # Fall back to local metrics
        
        avg_processing_time = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        
        return {
            'uptime_seconds': time.time() - self.start_time,
            'messages_processed': self.processed_count,
            'messages_failed': self.error_count,
            'success_rate': (self.processed_count - self.error_count) / max(self.processed_count, 1),
            'avg_processing_time_ms': avg_processing_time * 1000,
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'memory_used_mb': memory.used / (1024 * 1024)
        }
