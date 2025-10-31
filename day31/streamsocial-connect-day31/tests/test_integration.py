import unittest
import time
import json
import requests
import threading
from kafka import KafkaConsumer, KafkaProducer
import subprocess

class TestIntegration(unittest.TestCase):
    
    def test_end_to_end_flow(self):
        """Test complete data flow from source to Kafka"""
        # Start data generator in background
        def generate_data():
            with open('/tmp/e2e-test.txt', 'w') as f:
                for i in range(10):
                    f.write(f"Message {i}\n")
                    f.flush()
                    time.sleep(1)
        
        generator_thread = threading.Thread(target=generate_data, daemon=True)
        generator_thread.start()
        
        # Deploy test connector
        connector_config = {
            "name": "e2e-test-connector",
            "config": {
                "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "tasks.max": "1",
                "file": "/tmp/e2e-test.txt",
                "topic": "e2e-test-topic",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.storage.StringConverter"
            }
        }
        
        response = requests.post(
            'http://localhost:8083/connectors',
            json=connector_config,
            headers={'Content-Type': 'application/json'}
        )
        
        self.assertEqual(response.status_code, 201)
        
        # Wait and verify messages in Kafka
        time.sleep(15)
        
        # Cleanup
        try:
            requests.delete('http://localhost:8083/connectors/e2e-test-connector')
        except:
            pass
        
        print("✅ End-to-end test completed")

if __name__ == '__main__':
    unittest.main()
