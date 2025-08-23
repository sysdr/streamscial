import time
from typing import Dict, List
from collections import defaultdict, deque
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger()

class MetricsCollector:
    def __init__(self):
        self.counters = defaultdict(int)
        self.gauges = defaultdict(float)
        self.histograms = defaultdict(list)
        self.commit_lags = deque(maxlen=1000)
        self.processing_times = deque(maxlen=1000)
        self.error_rates = deque(maxlen=100)
        self.start_time = time.time()
    
    def increment_counter(self, name: str, value: int = 1):
        """Increment a counter metric"""
        self.counters[name] += value
        logger.debug("Counter incremented", metric=name, value=value)
    
    def set_gauge(self, name: str, value: float):
        """Set a gauge metric"""
        self.gauges[name] = value
        logger.debug("Gauge set", metric=name, value=value)
    
    def record_histogram(self, name: str, value: float):
        """Record a histogram value"""
        self.histograms[name].append(value)
        if len(self.histograms[name]) > 1000:
            self.histograms[name] = self.histograms[name][-1000:]
    
    def record_commit_lag(self, offset: int):
        """Record commit lag for monitoring"""
        timestamp = time.time()
        self.commit_lags.append({'offset': offset, 'timestamp': timestamp})
    
    def record_processing_time(self, duration: float):
        """Record message processing time"""
        self.processing_times.append({
            'duration': duration,
            'timestamp': time.time()
        })
    
    def calculate_error_rate(self) -> float:
        """Calculate error rate over last minute"""
        total_processed = self.counters.get('engagements_processed', 0)
        total_errors = self.counters.get('processing_errors', 0)
        
        if total_processed == 0:
            return 0.0
        
        error_rate = (total_errors / (total_processed + total_errors)) * 100
        self.error_rates.append({
            'rate': error_rate,
            'timestamp': time.time()
        })
        
        return error_rate
    
    def get_throughput(self) -> float:
        """Calculate messages per second"""
        uptime = time.time() - self.start_time
        total_processed = self.counters.get('engagements_processed', 0)
        
        if uptime == 0:
            return 0.0
        
        return total_processed / uptime
    
    def get_avg_processing_time(self) -> float:
        """Get average processing time"""
        if not self.processing_times:
            return 0.0
        
        recent_times = [pt['duration'] for pt in list(self.processing_times)[-100:]]
        return sum(recent_times) / len(recent_times)
    
    def get_metrics_summary(self) -> Dict:
        """Get comprehensive metrics summary"""
        return {
            'counters': dict(self.counters),
            'gauges': dict(self.gauges),
            'throughput_mps': self.get_throughput(),
            'error_rate_percent': self.calculate_error_rate(),
            'avg_processing_time_ms': self.get_avg_processing_time() * 1000,
            'uptime_seconds': time.time() - self.start_time,
            'commit_lag_count': len(self.commit_lags)
        }
    
    def reset_metrics(self):
        """Reset all metrics"""
        self.counters.clear()
        self.gauges.clear()
        self.histograms.clear()
        self.commit_lags.clear()
        self.processing_times.clear()
        self.error_rates.clear()
        self.start_time = time.time()
        logger.info("Metrics reset")
