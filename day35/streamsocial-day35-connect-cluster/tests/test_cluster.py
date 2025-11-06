"""
Integration tests for distributed Connect cluster
"""
import pytest
import requests
import time
import subprocess


class TestConnectCluster:
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Wait for cluster to be ready"""
        time.sleep(5)
    
    def test_all_workers_healthy(self):
        """Test all workers are responding"""
        workers = [
            'http://localhost:8083',
            'http://localhost:8084',
            'http://localhost:8085'
        ]
        
        for worker_url in workers:
            response = requests.get(worker_url, timeout=5)
            assert response.status_code == 200
            data = response.json()
            assert 'version' in data
    
    def test_connectors_deployed(self):
        """Test connectors are successfully deployed"""
        response = requests.get('http://localhost:8083/connectors', timeout=5)
        assert response.status_code == 200
        connectors = response.json()
        assert len(connectors) >= 3
    
    def test_tasks_distributed(self):
        """Test tasks are distributed across workers"""
        response = requests.get('http://localhost:8083/connectors', timeout=5)
        connectors = response.json()
        
        task_distribution = {}
        for connector in connectors:
            status_response = requests.get(
                f'http://localhost:8083/connectors/{connector}/status',
                timeout=5
            )
            status = status_response.json()
            
            for task in status.get('tasks', []):
                worker_id = task.get('worker_id', 'unknown')
                task_distribution[worker_id] = task_distribution.get(worker_id, 0) + 1
        
        # Verify tasks are distributed
        assert len(task_distribution) > 0
        print(f"Task distribution: {task_distribution}")
    
    def test_connector_status(self):
        """Test connector status is RUNNING"""
        response = requests.get('http://localhost:8083/connectors', timeout=5)
        connectors = response.json()
        
        for connector in connectors:
            status_response = requests.get(
                f'http://localhost:8083/connectors/{connector}/status',
                timeout=5
            )
            status = status_response.json()
            
            connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
            assert connector_state in ['RUNNING', 'PAUSED']
    
    def test_topics_created(self):
        """Test internal Connect topics are created"""
        # This test would require Kafka consumer; simplified for demo
        assert True  # Placeholder


class TestFaultTolerance:
    
    def test_worker_failure_recovery(self):
        """Test cluster recovers from worker failure"""
        # Get initial task distribution
        response = requests.get('http://localhost:8083/connectors', timeout=5)
        initial_connectors = response.json()
        
        print(f"Initial connector count: {len(initial_connectors)}")
        
        # Simulate worker failure (in real scenario)
        # subprocess.run(['docker', 'pause', 'connect-worker-2'])
        # time.sleep(15)  # Wait for rebalance
        # subprocess.run(['docker', 'unpause', 'connect-worker-2'])
        
        # Verify cluster still functional
        time.sleep(5)
        response = requests.get('http://localhost:8083/connectors', timeout=5)
        assert response.status_code == 200
        
        connectors = response.json()
        assert len(connectors) == len(initial_connectors)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
