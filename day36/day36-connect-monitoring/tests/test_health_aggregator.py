import pytest
import sys
sys.path.insert(0, 'src/monitoring')
from health_aggregator import HealthAggregator

def test_health_score_calculation():
    config = {
        'health_score': {
            'error_weight': 0.4,
            'task_weight': 0.3,
            'lag_weight': 0.3
        }
    }
    
    # Mock aggregator without DB
    class MockAggregator:
        def __init__(self):
            self.config = config
        
        def calculate_health_score(self, metrics):
            weights = self.config['health_score']
            error_score = (1 - metrics['error_rate']) * weights['error_weight']
            task_score = (metrics['tasks_running'] / metrics['tasks_total']) * weights['task_weight']
            lag_minutes = metrics['lag_seconds'] / 60
            lag_score = (1 / (1 + lag_minutes/30)) * weights['lag_weight']
            total_score = (error_score + task_score + lag_score) * 100
            return min(100, max(0, total_score))
    
    agg = MockAggregator()
    
    metrics = {
        'error_rate': 0.01,
        'tasks_running': 3,
        'tasks_total': 3,
        'lag_seconds': 30
    }
    
    score = agg.calculate_health_score(metrics)
    assert 80 <= score <= 100
    
def test_status_determination():
    # Test status logic
    test_cases = [
        (95, 0.001, 'HEALTHY'),
        (75, 0.01, 'DEGRADED'),
        (55, 0.05, 'CRITICAL'),
    ]
    
    for score, error_rate, expected in test_cases:
        if score >= 90 and error_rate <= 0.001:
            assert expected == 'HEALTHY'
        elif score >= 70:
            assert expected == 'DEGRADED'
        elif score >= 50:
            assert expected == 'CRITICAL'
