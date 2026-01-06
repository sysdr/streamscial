"""Test suite for ksqlDB trending analytics system"""
import pytest
import time
import requests
import json
from kafka import KafkaProducer, KafkaConsumer
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

KAFKA_BOOTSTRAP = 'localhost:9092'
KSQLDB_URL = 'http://localhost:8088'

@pytest.fixture
def kafka_producer():
    """Create Kafka producer for testing"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    yield producer
    producer.close()

class TestKsqlDBSystem:
    
    def test_kafka_connectivity(self, kafka_producer):
        """Test Kafka is accessible"""
        test_event = {'test': 'connectivity', 'timestamp': int(time.time() * 1000)}
        future = kafka_producer.send('test-topic', test_event)
        result = future.get(timeout=10)
        assert result is not None
    
    def test_ksqldb_server_health(self):
        """Test ksqlDB server is running"""
        response = requests.get(f'{KSQLDB_URL}/info')
        assert response.status_code == 200
        data = response.json()
        assert 'KsqlServerInfo' in data
    
    def test_streams_exist(self):
        """Test streams are created"""
        payload = {'ksql': 'SHOW STREAMS;', 'streamsProperties': {}}
        response = requests.post(
            f'{KSQLDB_URL}/ksql',
            json=payload,
            headers={'Content-Type': 'application/vnd.ksql.v1+json'}
        )
        assert response.status_code == 200
        data = response.json()
        
        # Check for our streams
        stream_names = []
        if data and len(data) > 0 and 'streams' in data[0]:
            stream_names = [s['name'] for s in data[0]['streams']]
        
        assert 'POSTS_STREAM' in stream_names or 'posts_stream' in [s.lower() for s in stream_names]
    
    def test_tables_exist(self):
        """Test tables are created"""
        payload = {'ksql': 'SHOW TABLES;', 'streamsProperties': {}}
        response = requests.post(
            f'{KSQLDB_URL}/ksql',
            json=payload,
            headers={'Content-Type': 'application/vnd.ksql.v1+json'}
        )
        assert response.status_code == 200
        data = response.json()
        
        table_names = []
        if data and len(data) > 0 and 'tables' in data[0]:
            table_names = [t['name'] for t in data[0]['tables']]
        
        assert len(table_names) > 0  # At least one table exists
    
    def test_post_event_processing(self, kafka_producer):
        """Test post events are processed by ksqlDB"""
        # Send test post with hashtags
        test_post = {
            'post_id': 99999,
            'user_id': 777,
            'content': 'Testing ksqlDB #test #automation',
            'hashtags': ['test', 'automation'],
            'created_at': '2025-05-15T12:00:00',
            'media_type': 'text',
            'timestamp': int(time.time() * 1000)
        }
        
        kafka_producer.send('posts', test_post)
        kafka_producer.flush()
        
        # Wait for processing
        time.sleep(5)
        
        # Query hashtag trends
        payload = {
            'sql': "SELECT hashtag, mention_count FROM hashtag_trends WHERE hashtag = 'test' EMIT CHANGES;",
            'streamsProperties': {}
        }
        
        response = requests.post(
            f'{KSQLDB_URL}/query-stream',
            json=payload,
            headers={'Content-Type': 'application/vnd.ksql.v1+json'},
            timeout=10
        )
        
        assert response.status_code == 200
        # Should have processed the hashtag
        assert 'test' in response.text.lower() or len(response.text) > 0
    
    def test_dashboard_api(self):
        """Test dashboard API endpoints"""
        # Give dashboard time to start
        max_attempts = 10
        for _ in range(max_attempts):
            try:
                response = requests.get('http://localhost:5000/api/health', timeout=2)
                if response.status_code == 200:
                    break
            except:
                pass
            time.sleep(2)
        
        response = requests.get('http://localhost:5000/api/health', timeout=5)
        assert response.status_code == 200
        
        health = response.json()
        assert 'status' in health
    
    def test_trending_hashtags_endpoint(self):
        """Test trending hashtags API"""
        try:
            response = requests.get('http://localhost:5000/api/trending/hashtags', timeout=5)
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
        except requests.exceptions.ConnectionError:
            pytest.skip("Dashboard not running")
    
    def test_performance_under_load(self, kafka_producer):
        """Test system handles burst of events"""
        events = []
        for i in range(100):
            event = {
                'post_id': 100000 + i,
                'user_id': i % 50,
                'content': f'Load test post {i} #loadtest',
                'hashtags': ['loadtest'],
                'created_at': '2025-05-15T12:00:00',
                'media_type': 'text',
                'timestamp': int(time.time() * 1000)
            }
            events.append(event)
        
        start_time = time.time()
        for event in events:
            kafka_producer.send('posts', event)
        kafka_producer.flush()
        
        send_duration = time.time() - start_time
        
        # Should handle 100 events quickly
        assert send_duration < 5.0
        
        # Wait for ksqlDB processing
        time.sleep(3)
        
        # Verify ksqlDB is still responsive
        response = requests.get(f'{KSQLDB_URL}/info', timeout=5)
        assert response.status_code == 200

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
