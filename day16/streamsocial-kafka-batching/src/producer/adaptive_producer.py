import asyncio
import time
import logging
import threading
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, Gauge
from config.kafka_config import KafkaConfig
from src.models.post_event import PostEvent

# Prometheus metrics
MESSAGES_SENT = Counter('kafka_messages_sent_total', 'Total messages sent', ['topic', 'config'])
SEND_DURATION = Histogram('kafka_send_duration_seconds', 'Message send duration', ['topic'])
BATCH_SIZE_GAUGE = Gauge('kafka_current_batch_size', 'Current batch size configuration')
BUFFER_USAGE = Gauge('kafka_buffer_usage_percent', 'Buffer memory usage percentage')

class AdaptiveKafkaProducer:
    def __init__(self, initial_config: str = 'adaptive'):
        self.config_name = initial_config
        self.config = KafkaConfig.CONFIGS[initial_config].copy()
        self.producer = None
        self.metrics = {
            'messages_per_second': 0,
            'avg_latency': 0,
            'buffer_usage': 0,
            'batch_efficiency': 0
        }
        self.running = False
        self.monitor_thread = None
        self.last_config_update = 0  # Timestamp of last config update
        self._setup_logging()
        self._initialize_producer()
        
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def _initialize_producer(self):
        producer_config = {
            'bootstrap_servers': KafkaConfig.BOOTSTRAP_SERVERS,
            'key_serializer': lambda x: x.encode('utf-8') if x else None,
            'value_serializer': lambda x: x.encode('utf-8') if isinstance(x, str) else x,
            **self.config
        }
        
        try:
            self.producer = KafkaProducer(**producer_config)
            self.logger.info(f"Producer initialized with config: {self.config_name}")
        except Exception as e:
            self.logger.error(f"Failed to initialize producer: {e}")
            raise
            
    def send_message(self, topic: str, message: PostEvent, key: Optional[str] = None) -> bool:
        if not self.producer or not self.running:
            return False
            
        try:
            start_time = time.time()
            partition_key = key or message.get_partition_key()
            
            future = self.producer.send(
                topic, 
                value=message.to_json(),
                key=partition_key
            )
            
            # Non-blocking send with callback
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            duration = time.time() - start_time
            SEND_DURATION.labels(topic=topic).observe(duration)
            MESSAGES_SENT.labels(topic=topic, config=self.config_name).inc()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False
            
    def _on_send_success(self, record_metadata):
        self.logger.debug(f"Message sent to {record_metadata.topic}:{record_metadata.partition}")
        
    def _on_send_error(self, error):
        self.logger.error(f"Message send failed: {error}")
        
    def start_monitoring(self):
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_performance)
        self.monitor_thread.start()
        
    def _monitor_performance(self):
        last_messages = 0
        last_time = time.time()
        
        while self.running:
            try:
                # Get producer metrics
                metrics = self.producer.metrics()
                
                # Calculate messages per second
                current_messages = sum(m.value for m in metrics.values() 
                                     if 'record-send-total' in str(m))
                current_time = time.time()
                time_diff = current_time - last_time
                
                if time_diff > 0:
                    self.metrics['messages_per_second'] = max(0, (current_messages - last_messages) / time_diff)
                else:
                    self.metrics['messages_per_second'] = 0
                    
                last_messages = current_messages
                last_time = current_time
                
                # Update Prometheus metrics
                BATCH_SIZE_GAUGE.set(self.config['batch_size'])
                
                # Buffer usage estimation
                buffer_usage = self._estimate_buffer_usage()
                BUFFER_USAGE.set(buffer_usage)
                self.metrics['buffer_usage'] = buffer_usage
                
                # Calculate average latency (simplified)
                self.metrics['avg_latency'] = self.config['linger_ms'] + 1  # Rough estimate
                
                # Calculate batch efficiency (simplified)
                self.metrics['batch_efficiency'] = min(100, (self.metrics['messages_per_second'] / 1000) * 10)
                
                # Adaptive configuration adjustment (temporarily disabled to prevent constant reinitialization)
                # self._adjust_configuration()
                
                time.sleep(1)  # Monitor every second
                
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                time.sleep(5)
                
    def _estimate_buffer_usage(self) -> float:
        # Estimate buffer usage based on producer internal state
        # In production, this would use actual producer metrics
        messages_per_sec = self.metrics['messages_per_second']
        avg_message_size = 2048  # 2KB average
        linger_ms = self.config['linger_ms']
        
        estimated_buffer_bytes = messages_per_sec * avg_message_size * (linger_ms / 1000)
        buffer_memory = self.config['buffer_memory']
        
        return min(100.0, (estimated_buffer_bytes / buffer_memory) * 100)
        
    def _adjust_configuration(self):
        messages_per_sec = self.metrics['messages_per_second']
        buffer_usage = self.metrics['buffer_usage']
        
        # Only adjust if we have meaningful data and significant changes are needed
        if messages_per_sec < 100:  # Not enough traffic to make decisions
            return
            
        # Adaptive scaling logic with more conservative thresholds
        if messages_per_sec > 50000 and buffer_usage < 30:
            # High traffic, increase batch size for efficiency
            new_batch_size = min(4194304, self.config['batch_size'] * 1.1)  # Smaller increments
            new_linger = min(50, self.config['linger_ms'] + 2)  # Smaller increments
            self._update_config(new_batch_size, new_linger)
            
        elif messages_per_sec < 5000 and buffer_usage > 90:
            # Low traffic, reduce batch size for latency
            new_batch_size = max(16384, self.config['batch_size'] * 0.9)  # Smaller decrements
            new_linger = max(0, self.config['linger_ms'] - 1)  # Smaller decrements
            self._update_config(new_batch_size, new_linger)
            
    def _update_config(self, batch_size: int, linger_ms: int):
        current_time = time.time()
        
        # Throttle config updates - only allow one update per 30 seconds
        if current_time - self.last_config_update < 30:
            return
            
        if (abs(self.config['batch_size'] - batch_size) > 1024 or 
            abs(self.config['linger_ms'] - linger_ms) > 1):
            
            self.config['batch_size'] = int(batch_size)
            self.config['linger_ms'] = int(linger_ms)
            self.last_config_update = current_time
            
            self.logger.info(f"Config updated: batch_size={batch_size}, linger_ms={linger_ms}")
            
            # Recreate producer with new config (in production, use producer.flush())
            self._reinitialize_producer()
            
    def _reinitialize_producer(self):
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
        except Exception as e:
            self.logger.warning(f"Error closing old producer: {e}")
        finally:
            self.producer = None
            self._initialize_producer()
        
    def flush(self):
        if self.producer:
            self.producer.flush()
            
    def close(self):
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        if self.producer:
            self.producer.close()
