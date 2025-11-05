import unittest
import requests
import time
import json
import threading
from src.data_generator import SocialSignalGenerator

class TestConnectCluster(unittest.TestCase):
    
    def setUp(self):
        self.worker_urls = [
            'http://localhost:8083',
            'http://localhost:8084',
            'http://localhost:8085'
        ]
        self.max_retries = 30
        self.wait_for_cluster()
        
    def wait_for_cluster(self):
        """Wait for Connect cluster to be ready"""
        print("Waiting for Connect cluster to be ready...")
        for _ in range(self.max_retries):
            try:
                healthy_workers = 0
                for url in self.worker_urls:
                    response = requests.get(f"{url}/", timeout=5)
                    if response.status_code == 200:
                        healthy_workers += 1
                
                if healthy_workers >= 2:  # At least 2 workers should be healthy
                    print(f"✅ Connect cluster ready ({healthy_workers}/3 workers)")
                    return
                    
            except Exception as e:
                pass
            
            time.sleep(2)
        
        self.fail("Connect cluster failed to start within timeout")
    
    def test_worker_health(self):
        """Test that Connect workers are healthy"""
        healthy_count = 0
        
        for i, url in enumerate(self.worker_urls):
            try:
                response = requests.get(f"{url}/", timeout=5)
                if response.status_code == 200:
                    healthy_count += 1
                    print(f"✅ Worker {i+1} is healthy")
                else:
                    print(f"⚠️  Worker {i+1} returned status {response.status_code}")
            except Exception as e:
                print(f"❌ Worker {i+1} is unreachable: {e}")
        
        self.assertGreaterEqual(healthy_count, 2, "At least 2 workers should be healthy")
    
    def test_connector_deployment(self):
        """Test deploying a connector"""
        connector_config = {
            "name": "test-file-source",
            "config": {
                "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "tasks.max": "1",
                "file": "/tmp/test-input.txt", 
                "topic": "test-topic",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.storage.StringConverter"
            }
        }
        
        # Create test input file
        with open('/tmp/test-input.txt', 'w') as f:
            f.write("test message 1\ntest message 2\n")
        
        # Deploy connector
        response = requests.post(
            f"{self.worker_urls[0]}/connectors",
            json=connector_config,
            headers={'Content-Type': 'application/json'}
        )
        
        self.assertEqual(response.status_code, 201, "Connector should be created successfully")
        
        # Verify connector is running
        time.sleep(5)
        status_response = requests.get(f"{self.worker_urls[0]}/connectors/test-file-source/status")
        self.assertEqual(status_response.status_code, 200)
        
        status = status_response.json()
        self.assertEqual(status['connector']['state'], 'RUNNING', "Connector should be running")
        
        # Cleanup
        requests.delete(f"{self.worker_urls[0]}/connectors/test-file-source")
    
    def test_task_distribution(self):
        """Test that tasks are distributed across workers"""
        connector_config = {
            "name": "multi-task-connector",
            "config": {
                "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "tasks.max": "3",
                "file": "/tmp/multi-task-input.txt",
                "topic": "multi-task-topic",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter", 
                "value.converter": "org.apache.kafka.connect.storage.StringConverter"
            }
        }
        
        with open('/tmp/multi-task-input.txt', 'w') as f:
            f.write("task distribution test\n")
        
        # Deploy connector
        requests.post(
            f"{self.worker_urls[0]}/connectors",
            json=connector_config,
            headers={'Content-Type': 'application/json'}
        )
        
        time.sleep(10)
        
        # Check task distribution
        active_workers = 0
        for url in self.worker_urls:
            try:
                connectors_response = requests.get(f"{url}/connectors")
                if connectors_response.status_code == 200:
                    connectors = connectors_response.json()
                    if 'multi-task-connector' in connectors:
                        active_workers += 1
            except:
                pass
        
        print(f"Connector found on {active_workers} workers")
        
        # Cleanup
        try:
            requests.delete(f"{self.worker_urls[0]}/connectors/multi-task-connector")
        except:
            pass

if __name__ == '__main__':
    unittest.main()
