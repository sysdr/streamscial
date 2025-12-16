import logging
from typing import List, Dict
from datetime import datetime, timedelta
from collections import deque
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ClusterMetrics:
    timestamp: datetime
    total_messages_per_sec: float
    total_bytes_in_per_sec: float
    total_bytes_out_per_sec: float
    avg_request_handler_idle: float
    avg_network_processor_idle: float
    total_under_replicated: int
    total_offline_partitions: int
    total_partitions: int
    broker_count: int
    health_status: str

class MetricsAggregator:
    def __init__(self, history_size: int = 360):  # 1 hour at 10s intervals
        self.history: deque = deque(maxlen=history_size)
        self.broker_history: Dict[int, deque] = {}
        
    def aggregate(self, broker_metrics: List) -> ClusterMetrics:
        """Aggregate metrics across all brokers"""
        if not broker_metrics:
            return ClusterMetrics(
                timestamp=datetime.now(),
                total_messages_per_sec=0,
                total_bytes_in_per_sec=0,
                total_bytes_out_per_sec=0,
                avg_request_handler_idle=0,
                avg_network_processor_idle=0,
                total_under_replicated=0,
                total_offline_partitions=0,
                total_partitions=0,
                broker_count=0,
                health_status='UNKNOWN'
            )
        
        # Aggregate totals
        total_messages = sum(m.messages_in_per_sec for m in broker_metrics)
        total_bytes_in = sum(m.bytes_in_per_sec for m in broker_metrics)
        total_bytes_out = sum(m.bytes_out_per_sec for m in broker_metrics)
        
        # Calculate averages
        avg_req_idle = sum(m.request_handler_idle_percent for m in broker_metrics) / len(broker_metrics)
        avg_net_idle = sum(m.network_processor_idle_percent for m in broker_metrics) / len(broker_metrics)
        
        # Aggregate counts
        total_under_rep = sum(m.under_replicated_partitions for m in broker_metrics)
        total_offline = sum(m.offline_partitions for m in broker_metrics)
        total_parts = sum(m.partition_count for m in broker_metrics)
        
        # Determine health status
        health = self._calculate_health(
            total_offline, total_under_rep, avg_req_idle, avg_net_idle
        )
        
        cluster_metrics = ClusterMetrics(
            timestamp=datetime.now(),
            total_messages_per_sec=total_messages,
            total_bytes_in_per_sec=total_bytes_in,
            total_bytes_out_per_sec=total_bytes_out,
            avg_request_handler_idle=avg_req_idle,
            avg_network_processor_idle=avg_net_idle,
            total_under_replicated=total_under_rep,
            total_offline_partitions=total_offline,
            total_partitions=total_parts,
            broker_count=len(broker_metrics),
            health_status=health
        )
        
        # Store in history
        self.history.append(cluster_metrics)
        
        # Store individual broker history
        for broker_metric in broker_metrics:
            if broker_metric.broker_id not in self.broker_history:
                self.broker_history[broker_metric.broker_id] = deque(maxlen=360)
            self.broker_history[broker_metric.broker_id].append(broker_metric)
        
        return cluster_metrics
    
    def _calculate_health(self, offline: int, under_rep: int, 
                         req_idle: float, net_idle: float) -> str:
        """Calculate overall cluster health status"""
        if offline > 0:
            return 'CRITICAL'
        elif under_rep > 0 or req_idle < 10 or net_idle < 10:
            return 'CRITICAL'
        elif under_rep == 0 and (req_idle < 20 or net_idle < 20):
            return 'WARNING'
        else:
            return 'HEALTHY'
    
    def get_history(self, minutes: int = 60) -> List[ClusterMetrics]:
        """Get cluster metrics history for the last N minutes"""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        return [m for m in self.history if m.timestamp >= cutoff]
    
    def get_broker_history(self, broker_id: int, minutes: int = 60):
        """Get specific broker's history"""
        if broker_id not in self.broker_history:
            return []
        cutoff = datetime.now() - timedelta(minutes=minutes)
        return [m for m in self.broker_history[broker_id] if m.timestamp >= cutoff]
