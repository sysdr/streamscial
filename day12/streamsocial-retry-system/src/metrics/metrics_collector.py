import time
import threading
from typing import Dict, Any
from collections import defaultdict, deque
from dataclasses import dataclass, asdict

@dataclass
class RetryMetrics:
    total_attempts: int = 0
    successful_retries: int = 0
    failed_retries: int = 0
    permanent_failures: int = 0
    circuit_breaker_trips: int = 0
    average_backoff_time: float = 0
    max_backoff_time: float = 0

class MetricsCollector:
    """Collects and aggregates retry and circuit breaker metrics"""
    
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.metrics = RetryMetrics()
        self.recent_events = deque(maxlen=window_size)
        self.lock = threading.RLock()
        
    def record_retry_attempt(self, success: bool, backoff_time: float, failure_type: str = None):
        """Record a retry attempt"""
        with self.lock:
            event = {
                'timestamp': time.time(),
                'type': 'retry',
                'success': success,
                'backoff_time': backoff_time,
                'failure_type': failure_type
            }
            
            self.recent_events.append(event)
            self.metrics.total_attempts += 1
            
            if success:
                self.metrics.successful_retries += 1
            else:
                self.metrics.failed_retries += 1
                if failure_type == 'permanent':
                    self.metrics.permanent_failures += 1
            
            # Update backoff statistics
            if backoff_time > 0:
                self.metrics.max_backoff_time = max(self.metrics.max_backoff_time, backoff_time)
                # Recalculate average
                total_backoff = sum(e.get('backoff_time', 0) for e in self.recent_events if e.get('backoff_time', 0) > 0)
                backoff_count = sum(1 for e in self.recent_events if e.get('backoff_time', 0) > 0)
                self.metrics.average_backoff_time = total_backoff / max(backoff_count, 1)
    
    def record_circuit_breaker_trip(self):
        """Record circuit breaker opening"""
        with self.lock:
            self.metrics.circuit_breaker_trips += 1
            event = {
                'timestamp': time.time(),
                'type': 'circuit_breaker_trip'
            }
            self.recent_events.append(event)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot"""
        with self.lock:
            return asdict(self.metrics)
    
    def get_recent_events(self, limit: int = 100) -> list:
        """Get recent events for debugging"""
        with self.lock:
            return list(self.recent_events)[-limit:]
    
    def reset_metrics(self):
        """Reset all metrics"""
        with self.lock:
            self.metrics = RetryMetrics()
            self.recent_events.clear()
