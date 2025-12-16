import logging
from typing import Dict, List
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)

class AlertLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

class Alert:
    def __init__(self, level: AlertLevel, metric: str, message: str, value: float):
        self.level = level
        self.metric = metric
        self.message = message
        self.value = value
        self.timestamp = datetime.now()
        self.acknowledged = False
    
    def to_dict(self):
        return {
            'level': self.level.value,
            'metric': self.metric,
            'message': self.message,
            'value': self.value,
            'timestamp': self.timestamp.isoformat(),
            'acknowledged': self.acknowledged
        }

class AlertManager:
    def __init__(self):
        self.active_alerts: List[Alert] = []
        self.alert_history: List[Alert] = []
        self.thresholds = {
            'offline_partitions': {'critical': 1, 'warning': 0},
            'under_replicated': {'critical': 5, 'warning': 1},
            'request_idle': {'critical': 10, 'warning': 20},
            'network_idle': {'critical': 10, 'warning': 20},
            'isr_shrinks': {'critical': 10, 'warning': 5}
        }
    
    def evaluate_metrics(self, cluster_metrics, broker_metrics: List) -> List[Alert]:
        """Evaluate metrics and generate alerts"""
        new_alerts = []
        
        # Check cluster-level metrics
        if cluster_metrics.total_offline_partitions >= self.thresholds['offline_partitions']['critical']:
            new_alerts.append(Alert(
                AlertLevel.CRITICAL,
                'offline_partitions',
                f'Critical: {cluster_metrics.total_offline_partitions} offline partitions detected',
                cluster_metrics.total_offline_partitions
            ))
        
        if cluster_metrics.total_under_replicated >= self.thresholds['under_replicated']['critical']:
            new_alerts.append(Alert(
                AlertLevel.CRITICAL,
                'under_replicated_partitions',
                f'Critical: {cluster_metrics.total_under_replicated} under-replicated partitions',
                cluster_metrics.total_under_replicated
            ))
        elif cluster_metrics.total_under_replicated >= self.thresholds['under_replicated']['warning']:
            new_alerts.append(Alert(
                AlertLevel.WARNING,
                'under_replicated_partitions',
                f'Warning: {cluster_metrics.total_under_replicated} under-replicated partitions',
                cluster_metrics.total_under_replicated
            ))
        
        if cluster_metrics.avg_request_handler_idle < self.thresholds['request_idle']['critical']:
            new_alerts.append(Alert(
                AlertLevel.CRITICAL,
                'request_handler_idle',
                f'Critical: Request handler idle at {cluster_metrics.avg_request_handler_idle:.1f}%',
                cluster_metrics.avg_request_handler_idle
            ))
        elif cluster_metrics.avg_request_handler_idle < self.thresholds['request_idle']['warning']:
            new_alerts.append(Alert(
                AlertLevel.WARNING,
                'request_handler_idle',
                f'Warning: Request handler idle at {cluster_metrics.avg_request_handler_idle:.1f}%',
                cluster_metrics.avg_request_handler_idle
            ))
        
        # Check broker-specific metrics
        for broker in broker_metrics:
            if broker.isr_shrinks_per_sec > self.thresholds['isr_shrinks']['critical']:
                new_alerts.append(Alert(
                    AlertLevel.CRITICAL,
                    f'broker_{broker.broker_id}_isr_shrinks',
                    f'Broker {broker.broker_id}: High ISR shrink rate ({broker.isr_shrinks_per_sec:.2f}/s)',
                    broker.isr_shrinks_per_sec
                ))
        
        # Update active alerts
        self.active_alerts = new_alerts
        self.alert_history.extend(new_alerts)
        
        # Keep history limited
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]
        
        return new_alerts
    
    def get_active_alerts(self) -> List[Dict]:
        """Get all active alerts"""
        return [a.to_dict() for a in self.active_alerts]
    
    def get_alert_summary(self) -> Dict:
        """Get summary of alerts by level"""
        summary = {
            'critical': len([a for a in self.active_alerts if a.level == AlertLevel.CRITICAL]),
            'warning': len([a for a in self.active_alerts if a.level == AlertLevel.WARNING]),
            'info': len([a for a in self.active_alerts if a.level == AlertLevel.INFO]),
            'total': len(self.active_alerts)
        }
        return summary
