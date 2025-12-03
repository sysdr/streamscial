"""Monitoring and metrics collection using peek operations."""

import time
import urllib.request
import re
from collections import defaultdict, deque
from threading import Lock
from prometheus_client import Counter, Histogram, Gauge, start_http_server, REGISTRY

class StreamMetrics:
    """Collect and expose stream processing metrics."""
    
    def __init__(self):
        self.lock = Lock()
        
        # Prometheus metrics
        self.posts_received = Counter(
            'posts_received_total',
            'Total posts received'
        )
        self.posts_after_spam = Counter(
            'posts_after_spam_filter_total',
            'Posts passing spam filter'
        )
        self.posts_after_policy = Counter(
            'posts_after_policy_filter_total',
            'Posts passing policy check'
        )
        self.posts_enriched = Counter(
            'posts_enriched_total',
            'Posts successfully enriched'
        )
        
        self.spam_detected = Counter(
            'spam_detected_total',
            'Spam posts detected',
            ['reason']
        )
        self.policy_violations = Counter(
            'policy_violations_total',
            'Policy violations detected',
            ['action']
        )
        
        self.processing_latency = Histogram(
            'processing_latency_seconds',
            'End-to-end processing latency',
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0]
        )
        
        self.throughput = Gauge(
            'posts_per_second',
            'Current processing throughput'
        )
        
        # Internal counters for dashboard
        self.counters = defaultdict(int)
        self.latencies = deque(maxlen=1000)
        self.timestamps = deque(maxlen=100)
        
        # Throughput tracking (class-level to persist across instances)
        if not hasattr(StreamMetrics, '_throughput_cache'):
            StreamMetrics._throughput_cache = {
                'last_total': 0,
                'last_time': time.time(),
                'samples': deque(maxlen=10)
            }
        
        # Start Prometheus HTTP server (only if not already running)
        try:
            start_http_server(8000)
        except OSError:
            # Port already in use, likely started by another process
            pass
    
    def record_received(self, post: dict):
        """Record post received (peek operation)."""
        with self.lock:
            self.posts_received.inc()
            self.counters['received'] += 1
            self.timestamps.append(time.time())
            post['_received_at'] = time.time()
    
    def record_spam_checked(self, post: dict, is_spam: bool, reason: str):
        """Record spam check result (peek operation)."""
        with self.lock:
            if not is_spam:
                self.posts_after_spam.inc()
                self.counters['after_spam'] += 1
            else:
                self.spam_detected.labels(reason=reason).inc()
                self.counters[f'spam_{reason}'] += 1
    
    def record_policy_checked(self, post: dict, action: str):
        """Record policy check result (peek operation)."""
        with self.lock:
            if action in ['ALLOW', 'DEPRIORITIZE']:
                self.posts_after_policy.inc()
                self.counters['after_policy'] += 1
            else:
                self.policy_violations.labels(action=action).inc()
                self.counters[f'policy_{action}'] += 1
    
    def record_enriched(self, post: dict):
        """Record enrichment completion (peek operation)."""
        with self.lock:
            self.posts_enriched.inc()
            self.counters['enriched'] += 1
            
            # Calculate latency if we have start time
            if '_received_at' in post:
                latency = time.time() - post['_received_at']
                self.processing_latency.observe(latency)
                self.latencies.append(latency)
    
    def _fetch_prometheus_metric(self, metric_name: str, labels: dict = None) -> float:
        """Fetch a metric value from the Prometheus HTTP endpoint."""
        try:
            url = 'http://localhost:8000/metrics'
            with urllib.request.urlopen(url, timeout=1) as response:
                content = response.read().decode('utf-8')
                
                # Build regex pattern
                if labels:
                    label_str = ','.join([f'{k}="{v}"' for k, v in labels.items()])
                    pattern = rf'{metric_name}\{{[^}}]*{re.escape(label_str)}[^}}]*\}}\s+([\d.eE+-]+)'
                else:
                    pattern = rf'{metric_name}\s+([\d.eE+-]+)'
                
                match = re.search(pattern, content)
                if match:
                    return float(match.group(1))
        except Exception:
            pass
        return 0.0
    
    def _fetch_histogram_buckets(self, metric_name: str) -> list:
        """Fetch histogram bucket values from Prometheus."""
        try:
            url = 'http://localhost:8000/metrics'
            with urllib.request.urlopen(url, timeout=1) as response:
                content = response.read().decode('utf-8')
                
                buckets = []
                pattern = rf'{metric_name}_bucket{{le="([\d.]+)"}}\s+([\d.e+-]+)'
                for match in re.finditer(pattern, content):
                    le = float(match.group(1))
                    count = float(match.group(2))
                    buckets.append((le, count))
                
                # Sort by le value
                buckets.sort(key=lambda x: x[0])
                return buckets
        except Exception:
            pass
        return []
    
    def _calculate_percentile_from_buckets(self, buckets: list, percentile: float) -> float:
        """Calculate percentile from histogram buckets."""
        if not buckets:
            return 0.0
        
        # Get total count
        total_count = buckets[-1][1] if buckets else 0
        if total_count == 0:
            return 0.0
        
        # Find the bucket containing the percentile
        target_count = total_count * percentile
        
        for i, (le, count) in enumerate(buckets):
            if count >= target_count:
                # Use the bucket's upper bound as the percentile estimate
                return le * 1000  # Convert to milliseconds
        
        # If not found, return the highest bucket
        return buckets[-1][0] * 1000
    
    def get_stats(self) -> dict:
        """Get current statistics for dashboard."""
        # Try to read from local counters first (if this is the stream processor)
        with self.lock:
            local_total = self.counters.get('received', 0)
            local_after_spam = self.counters.get('after_spam', 0)
            local_after_policy = self.counters.get('after_policy', 0)
            local_enriched = self.counters.get('enriched', 0)
            local_latencies = list(self.latencies) if self.latencies else []
            local_timestamps = list(self.timestamps) if self.timestamps else []
        
        # Fetch from Prometheus metrics endpoint (works across processes)
        total = self._fetch_prometheus_metric('posts_received_total') or local_total
        after_spam = self._fetch_prometheus_metric('posts_after_spam_filter_total') or local_after_spam
        after_policy = self._fetch_prometheus_metric('posts_after_policy_filter_total') or local_after_policy
        enriched = self._fetch_prometheus_metric('posts_enriched_total') or local_enriched
        
        # Calculate throughput from recent timestamps or from counter rate
        throughput = 0.0
        now = time.time()
        
        with self.lock:
            if local_timestamps:
                # Use recent timestamps for accurate throughput
                recent_timestamps = [t for t in local_timestamps if now - t < 10]
                throughput = len(recent_timestamps) / 10 if recent_timestamps else 0
            else:
                # Calculate throughput from rate of change in total counter
                cache = StreamMetrics._throughput_cache
                time_diff = now - cache['last_time']
                
                # Calculate if we have at least 1 second difference
                if time_diff >= 1.0:
                    count_diff = total - cache['last_total']
                    
                    # Only calculate if count increased (new data arrived)
                    if count_diff > 0:
                        current_throughput = count_diff / time_diff
                        
                        # Cap throughput at reasonable maximum (2000 posts/sec) to avoid startup spikes
                        if current_throughput <= 2000:
                            cache['samples'].append(current_throughput)
                            # Keep only last 10 samples
                            if len(cache['samples']) > 10:
                                cache['samples'].popleft()
                        
                        cache['last_total'] = total
                        cache['last_time'] = now
                    elif time_diff > 30:
                        # If no new data for 30 seconds, reset to show 0
                        cache['samples'].clear()
                
                # Use average of recent samples for smoother display
                if cache['samples']:
                    samples = list(cache['samples'])
                    # Use simple average, but filter extreme outliers
                    if len(samples) > 2:
                        # Remove top and bottom 10% if we have enough samples
                        sorted_samples = sorted(samples)
                        start_idx = max(1, len(sorted_samples) // 10)
                        end_idx = len(sorted_samples) - start_idx
                        filtered = sorted_samples[start_idx:end_idx]
                        throughput = sum(filtered) / len(filtered) if filtered else 0
                    else:
                        throughput = sum(samples) / len(samples)
                
                # Update the Prometheus gauge
                if throughput > 0:
                    self.throughput.set(throughput)
        
        # Calculate spam rate
        spam_count = total - after_spam
        spam_rate = (spam_count / total * 100) if total > 0 else 0
        
        # Calculate policy violation rate
        policy_count = after_spam - after_policy
        policy_rate = (policy_count / total * 100) if total > 0 else 0
        
        # Get latency percentiles from histogram buckets or local data
        p50 = p95 = p99 = 0.0
        with self.lock:
            if local_latencies and len(local_latencies) > 0:
                latencies_sorted = sorted(local_latencies)
                p50 = latencies_sorted[len(latencies_sorted) // 2] * 1000
                if len(latencies_sorted) > 1:
                    p95_idx = int(len(latencies_sorted) * 0.95)
                    p99_idx = int(len(latencies_sorted) * 0.99)
                    p95 = latencies_sorted[min(p95_idx, len(latencies_sorted) - 1)] * 1000
                    p99 = latencies_sorted[min(p99_idx, len(latencies_sorted) - 1)] * 1000
        
        # If no local latencies, try to get from Prometheus histogram
        if p50 == 0 and p95 == 0 and p99 == 0:
            buckets = self._fetch_histogram_buckets('processing_latency_seconds')
            if buckets:
                p50 = self._calculate_percentile_from_buckets(buckets, 0.50)
                p95 = self._calculate_percentile_from_buckets(buckets, 0.95)
                p99 = self._calculate_percentile_from_buckets(buckets, 0.99)
        
        # Build counters dict
        counters = {
            'received': int(total),
            'after_spam': int(after_spam),
            'after_policy': int(after_policy),
            'enriched': int(enriched)
        }
        
        # Try to fetch spam and policy violation counters
        spam_reasons = ['keywords', 'ml_score', 'emoji_count']
        for reason in spam_reasons:
            spam_val = self._fetch_prometheus_metric('spam_detected_total', {'reason': reason})
            if spam_val > 0:
                counters[f'spam_{reason}'] = int(spam_val)
        
        policy_actions = ['REMOVE', 'FLAG', 'DEPRIORITIZE']
        for action in policy_actions:
            policy_val = self._fetch_prometheus_metric('policy_violations_total', {'action': action})
            if policy_val > 0:
                counters[f'policy_{action}'] = int(policy_val)
        
        return {
            'throughput': round(throughput, 2),
            'spam_rate': round(spam_rate, 2),
            'policy_violation_rate': round(policy_rate, 2),
            'total_processed': int(total),
            'spam_filtered': int(spam_count),
            'policy_violations': int(policy_count),
            'latency_p50_ms': round(p50, 2),
            'latency_p95_ms': round(p95, 2),
            'latency_p99_ms': round(p99, 2),
            'counters': counters
        }


# Global metrics instance
metrics = StreamMetrics()
