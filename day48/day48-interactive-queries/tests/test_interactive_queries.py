import pytest
import time
import requests
from kafka import KafkaProducer
import json

BOOTSTRAP_SERVERS = 'localhost:9092'
API_URL = 'http://localhost:8080'

@pytest.fixture
def kafka_producer():
    """Create Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )
    yield producer
    producer.close()

def test_api_health():
    """Test health endpoint"""
    response = requests.get(f'{API_URL}/api/health')
    assert response.status_code == 200
    data = response.json()
    assert data['status'] == 'healthy'
    assert 'instance' in data

def test_trending_endpoint():
    """Test trending endpoint"""
    response = requests.get(f'{API_URL}/api/trending')
    assert response.status_code == 200
    data = response.json()
    assert 'trending' in data
    assert 'timestamp' in data

def test_query_with_posts(kafka_producer):
    """Test queries after posting"""
    # Send posts with hashtags
    posts = [
        {'text': '#AI #MachineLearning rocks!', 'timestamp': int(time.time() * 1000)},
        {'text': 'Love #AI and #DataScience', 'timestamp': int(time.time() * 1000)},
        {'text': '#AI is the future', 'timestamp': int(time.time() * 1000)}
    ]
    
    for post in posts:
        kafka_producer.send('social.posts', post)
    kafka_producer.flush()
    
    # Wait for processing
    time.sleep(5)
    
    # Query trending
    response = requests.get(f'{API_URL}/api/trending')
    assert response.status_code == 200
    data = response.json()
    
    # Should have AI as trending
    trending_hashtags = [t['hashtag'] for t in data['trending']]
    assert 'ai' in trending_hashtags

def test_specific_hashtag_query():
    """Test querying specific hashtag"""
    response = requests.get(f'{API_URL}/api/trending/ai')
    assert response.status_code == 200
    data = response.json()
    assert data['hashtag'] == 'ai'
    assert 'stats' in data

def test_metadata_endpoint():
    """Test metadata endpoint"""
    response = requests.get(f'{API_URL}/api/metadata')
    assert response.status_code == 200
    data = response.json()
    assert 'instances' in data
    assert 'total_instances' in data
    assert data['total_instances'] >= 1

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
