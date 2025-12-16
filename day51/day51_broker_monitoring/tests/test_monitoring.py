import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.collectors.jmx_collector import JMXCollector, BrokerMetrics
from src.aggregators.cluster_aggregator import MetricsAggregator
from src.alerts.alert_manager import AlertManager, AlertLevel
from datetime import datetime

class TestJMXCollector:
    def test_parse_prometheus_metrics(self):
        collector = JMXCollector([])
        
        metrics_text = """
# HELP kafka_server_brokertopicmetrics_messagesinpersec
kafka_server_brokertopicmetrics_messagesinpersec 1234.5
kafka_server_replicamanager_underreplicatedpartitions 0
kafka_network_socketserver_networkprocessoravgidlepercent 85.5
        """
        
        parsed = collector._parse_prometheus_metrics(metrics_text)
        
        assert parsed['kafka_server_brokertopicmetrics_messagesinpersec'] == 1234.5
        assert parsed['kafka_server_replicamanager_underreplicatedpartitions'] == 0
        assert parsed['kafka_network_socketserver_networkprocessoravgidlepercent'] == 85.5
    
    def test_calculate_rates(self):
        collector = JMXCollector([])
        
        # First call - no previous data
        current_metrics = {'kafka_server_brokertopicmetrics_messagesinpersec': 1000}
        rates = collector._calculate_rates(1, current_metrics, 100.0)
        assert rates == {}
        
        # Second call - should calculate rate
        current_metrics = {'kafka_server_brokertopicmetrics_messagesinpersec': 2000}
        rates = collector._calculate_rates(1, current_metrics, 110.0)
        
        # Rate = (2000 - 1000) / (110 - 100) = 100 msg/s
        assert 'messages_in' in rates
        assert rates['messages_in'] == pytest.approx(100.0, rel=0.1)

class TestMetricsAggregator:
    def test_aggregate_empty_metrics(self):
        aggregator = MetricsAggregator()
        result = aggregator.aggregate([])
        
        assert result.broker_count == 0
        assert result.total_messages_per_sec == 0
        assert result.health_status == 'UNKNOWN'
    
    def test_aggregate_multiple_brokers(self):
        aggregator = MetricsAggregator()
        
        broker1 = BrokerMetrics(
            broker_id=1,
            timestamp=datetime.now(),
            messages_in_per_sec=1000.0,
            bytes_in_per_sec=1024000.0,
            request_handler_idle_percent=50.0,
            partition_count=100
        )
        
        broker2 = BrokerMetrics(
            broker_id=2,
            timestamp=datetime.now(),
            messages_in_per_sec=1500.0,
            bytes_in_per_sec=1536000.0,
            request_handler_idle_percent=60.0,
            partition_count=110
        )
        
        result = aggregator.aggregate([broker1, broker2])
        
        assert result.broker_count == 2
        assert result.total_messages_per_sec == 2500.0
        assert result.avg_request_handler_idle == 55.0
        assert result.total_partitions == 210
    
    def test_health_calculation(self):
        aggregator = MetricsAggregator()
        
        # Healthy
        assert aggregator._calculate_health(0, 0, 50, 50) == 'HEALTHY'
        
        # Warning
        assert aggregator._calculate_health(0, 0, 15, 50) == 'WARNING'
        
        # Critical
        assert aggregator._calculate_health(1, 0, 50, 50) == 'CRITICAL'
        assert aggregator._calculate_health(0, 5, 5, 50) == 'CRITICAL'

class TestAlertManager:
    def test_no_alerts_when_healthy(self):
        manager = AlertManager()
        
        broker = BrokerMetrics(
            broker_id=1,
            timestamp=datetime.now(),
            request_handler_idle_percent=50.0,
            under_replicated_partitions=0,
            offline_partitions=0
        )
        
        from src.aggregators.cluster_aggregator import ClusterMetrics
        cluster = ClusterMetrics(
            timestamp=datetime.now(),
            total_messages_per_sec=1000,
            total_bytes_in_per_sec=1024000,
            total_bytes_out_per_sec=512000,
            avg_request_handler_idle=50.0,
            avg_network_processor_idle=60.0,
            total_under_replicated=0,
            total_offline_partitions=0,
            total_partitions=100,
            broker_count=3,
            health_status='HEALTHY'
        )
        
        alerts = manager.evaluate_metrics(cluster, [broker])
        assert len(alerts) == 0
    
    def test_critical_alert_offline_partitions(self):
        manager = AlertManager()
        
        from src.aggregators.cluster_aggregator import ClusterMetrics
        cluster = ClusterMetrics(
            timestamp=datetime.now(),
            total_messages_per_sec=1000,
            total_bytes_in_per_sec=1024000,
            total_bytes_out_per_sec=512000,
            avg_request_handler_idle=50.0,
            avg_network_processor_idle=60.0,
            total_under_replicated=0,
            total_offline_partitions=5,  # Critical!
            total_partitions=100,
            broker_count=3,
            health_status='CRITICAL'
        )
        
        alerts = manager.evaluate_metrics(cluster, [])
        
        critical_alerts = [a for a in alerts if a.level == AlertLevel.CRITICAL]
        assert len(critical_alerts) > 0
        assert any('offline' in a.message.lower() for a in critical_alerts)
    
    def test_warning_alert_low_idle(self):
        manager = AlertManager()
        
        from src.aggregators.cluster_aggregator import ClusterMetrics
        cluster = ClusterMetrics(
            timestamp=datetime.now(),
            total_messages_per_sec=1000,
            total_bytes_in_per_sec=1024000,
            total_bytes_out_per_sec=512000,
            avg_request_handler_idle=15.0,  # Warning threshold
            avg_network_processor_idle=60.0,
            total_under_replicated=0,
            total_offline_partitions=0,
            total_partitions=100,
            broker_count=3,
            health_status='WARNING'
        )
        
        alerts = manager.evaluate_metrics(cluster, [])
        
        warning_alerts = [a for a in alerts if a.level == AlertLevel.WARNING]
        assert len(warning_alerts) > 0

def test_integration_flow():
    """Test complete metrics flow"""
    # This would require running Kafka - mark as integration test
    pass

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
