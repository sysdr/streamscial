import pytest
import sys
sys.path.insert(0, 'src/monitoring')
from metrics_collector import ConnectMetricsCollector

def test_metrics_setup():
    collector = ConnectMetricsCollector(['http://localhost:8083'])
    assert collector.connector_status is not None
    assert collector.task_status is not None
    assert collector.throughput is not None

def test_simulate_metrics():
    collector = ConnectMetricsCollector(['http://localhost:8083'])
    collector.simulate_connector_metrics()
    # Test passes if no exception raised

def test_system_metrics():
    collector = ConnectMetricsCollector(['http://localhost:8083'])
    collector.collect_system_metrics()
    # Test passes if no exception raised
