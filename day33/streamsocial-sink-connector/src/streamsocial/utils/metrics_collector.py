"""
Metrics collection for sink connector monitoring
"""
import time
import threading
from typing import Dict, Any
from dataclasses import dataclass, field
from collections import defaultdict

@dataclass
class ConnectorMetrics:
    """Metrics for connector performance monitoring"""
    messages_processed: int = 0
    batches_flushed: int = 0
    processing_errors: int = 0
    batch_failures: int = 0
    total_flush_time: float = 0.0
    avg_batch_size: float = 0.0
    start_time: float = field(default_factory=time.time)
    
    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

class MetricsCollector:
    """Thread-safe metrics collector"""
    
    def __init__(self):
        self._metrics = ConnectorMetrics()
        self._lock = threading.Lock()
        self._batch_sizes = []
        self._flush_times = []
        
    def increment_messages_processed(self):
        with self._lock:
            self._metrics.messages_processed += 1
            
    def increment_processing_errors(self):
        with self._lock:
            self._metrics.processing_errors += 1
            
    def increment_error_count(self):
        with self._lock:
            self._metrics.processing_errors += 1
            
    def increment_batch_failures(self):
        with self._lock:
            self._metrics.batch_failures += 1
            
    def record_batch_flush(self, batch_size: int, flush_time: float):
        with self._lock:
            self._metrics.batches_flushed += 1
            self._metrics.total_flush_time += flush_time
            self._batch_sizes.append(batch_size)
            self._flush_times.append(flush_time)
            
            # Calculate average batch size
            self._metrics.avg_batch_size = sum(self._batch_sizes) / len(self._batch_sizes)
            
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics as dictionary"""
        with self._lock:
            avg_flush_time = (
                self._metrics.total_flush_time / self._metrics.batches_flushed 
                if self._metrics.batches_flushed > 0 else 0.0
            )
            
            throughput = (
                self._metrics.messages_processed / self._metrics.uptime_seconds()
                if self._metrics.uptime_seconds() > 0 else 0.0
            )
            
            return {
                'messages_processed': self._metrics.messages_processed,
                'batches_flushed': self._metrics.batches_flushed,
                'processing_errors': self._metrics.processing_errors,
                'batch_failures': self._metrics.batch_failures,
                'avg_batch_size': self._metrics.avg_batch_size,
                'avg_flush_time_seconds': avg_flush_time,
                'throughput_messages_per_second': throughput,
                'uptime_seconds': self._metrics.uptime_seconds(),
                'error_rate': (
                    self._metrics.processing_errors / self._metrics.messages_processed
                    if self._metrics.messages_processed > 0 else 0.0
                )
            }
            
    def reset_metrics(self):
        """Reset all metrics"""
        with self._lock:
            self._metrics = ConnectorMetrics()
            self._batch_sizes.clear()
            self._flush_times.clear()
