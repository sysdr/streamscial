import pytest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.collectors.sla_checker import SLAChecker
import json

def test_sla_checker_healthy():
    """Test SLA checker with healthy metrics"""
    # Create temp config
    config = {
        "consumer_groups": {
            "test-group": {
                "max_lag_messages": 10000,
                "min_throughput_rps": 5000,
                "max_e2e_latency_ms": 100,
                "priority": "critical"
            }
        }
    }
    
    with open('/tmp/test_sla_config.json', 'w') as f:
        json.dump(config, f)
    
    checker = SLAChecker('/tmp/test_sla_config.json')
    result = checker.check_compliance('test-group', 500, 6000, 50)
    
    assert result['status'] == 'healthy'
    assert result['health_score'] > 90
    assert len(result['violations']) == 0

def test_sla_checker_critical():
    """Test SLA checker with critical metrics"""
    config = {
        "consumer_groups": {
            "test-group": {
                "max_lag_messages": 1000,
                "min_throughput_rps": 5000,
                "max_e2e_latency_ms": 100,
                "priority": "critical"
            }
        }
    }
    
    with open('/tmp/test_sla_config.json', 'w') as f:
        json.dump(config, f)
    
    checker = SLAChecker('/tmp/test_sla_config.json')
    result = checker.check_compliance('test-group', 5000, 2000, 200)
    
    assert result['status'] == 'critical'
    assert result['health_score'] < 70
    assert len(result['violations']) > 0

def test_lag_velocity_calculation():
    """Test lag velocity tracking"""
    config = {
        "consumer_groups": {
            "test-group": {
                "max_lag_messages": 10000,
                "min_throughput_rps": 5000,
                "max_e2e_latency_ms": 100,
                "priority": "critical"
            }
        }
    }
    
    with open('/tmp/test_sla_config.json', 'w') as f:
        json.dump(config, f)
    
    checker = SLAChecker('/tmp/test_sla_config.json')
    
    # First check
    result1 = checker.check_compliance('test-group', 1000, 6000, 50)
    
    # Simulate time passing with increasing lag
    import time
    time.sleep(1)
    
    result2 = checker.check_compliance('test-group', 2000, 6000, 50)
    
    assert 'lag_velocity' in result2
    # Lag increased, so velocity should be positive
    assert result2['lag_velocity'] >= 0

if __name__ == "__main__":
    pytest.main([__file__, '-v'])
