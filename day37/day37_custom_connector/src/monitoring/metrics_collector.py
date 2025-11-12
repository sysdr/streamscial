"""
Prometheus-style metrics collection
"""
import time
import threading
from typing import Dict, List, Any
from prometheus_client import Counter, Gauge, Histogram, start_http_server


class MetricsCollector:
    """Collects and exposes connector metrics"""
    
    def __init__(self, port: int = 8080):
        self.port = port
        
        # Define metrics
        self.api_calls = Counter(
            'social_connector_api_calls_total',
            'Total API calls made',
            ['platform', 'account_id']
        )
        
        self.records_produced = Counter(
            'social_connector_records_produced_total',
            'Total records produced to Kafka',
            ['platform', 'topic']
        )
        
        self.rate_limit_waits = Counter(
            'social_connector_rate_limit_waits_total',
            'Number of times rate limited',
            ['platform']
        )
        
        self.api_latency = Histogram(
            'social_connector_api_latency_seconds',
            'API call latency',
            ['platform']
        )
        
        self.available_tokens = Gauge(
            'social_connector_available_tokens',
            'Available rate limit tokens',
            ['platform']
        )
        
        self.task_status = Gauge(
            'social_connector_task_status',
            'Task status (1=running, 0=stopped)',
            ['task_id', 'platform', 'account_id']
        )
        
    def start_server(self):
        """Start Prometheus metrics server"""
        start_http_server(self.port)
        print(f"Metrics server started on port {self.port}")
        
    def update_from_task_metrics(self, task_metrics: Dict[str, Any]):
        """Update metrics from task"""
        platform = task_metrics.get('platform', 'unknown')
        account_id = task_metrics.get('account_id', 'unknown')
        metrics = task_metrics.get('metrics', {})
        
        # Update counters (only increment by difference)
        # In production, tasks would report incremental updates
        
        self.available_tokens.labels(platform=platform).set(
            task_metrics.get('available_tokens', 0)
        )
        
        self.task_status.labels(
            task_id=task_metrics.get('task_id'),
            platform=platform,
            account_id=account_id
        ).set(1)
