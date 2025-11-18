import pytest
import json
import time
from confluent_kafka import Producer, Consumer
from src.processors.feature_processor import FeatureProcessor

class TestFeatureProcessor:
    
    def setup_method(self):
        self.processor = FeatureProcessor('localhost:9092', 'test-processor')
    
    def test_extract_user_features(self):
        profile = {
            'user_id': 1,
            'username': 'test_user',
            'follower_count': 5000,
            'following_count': 500,
            'account_age_days': 365,
            'engagement_rate': 0.08,
            'interests': ['music', 'tech']
        }
        
        features = self.processor.extract_user_features(profile)
        
        assert features['user_id'] == 1
        assert features['follower_count_normalized'] == 0.5
        assert features['account_maturity'] == 1.0
        assert features['interest_count'] == 2
    
    def test_extract_content_features(self):
        metadata = {
            'content_id': 100,
            'creator_id': 1,
            'category': 'technology',
            'engagement_score': 50.0,
            'view_count': 10000,
            'tags': ['tech', 'ai']
        }
        
        features = self.processor.extract_content_features(metadata)
        
        assert features['content_id'] == 100
        assert features['engagement_score_normalized'] == 0.5
        assert features['category_encoded'] == 4
        assert features['tag_count'] == 2
    
    def test_extract_interaction_features(self):
        interaction = {
            'interaction_id': 1,
            'user_id': 1,
            'content_id': 100,
            'interaction_type': 'like',
            'duration_seconds': 60
        }
        
        user_features = {
            'follower_count_normalized': 0.5,
            'engagement_rate': 0.08,
            'account_maturity': 1.0
        }
        
        content_features = {
            'engagement_score_normalized': 0.5,
            'view_count_log': 0.5,
            'category_encoded': 4
        }
        
        features = self.processor.extract_interaction_features(
            interaction, user_features, content_features
        )
        
        assert features['interaction_type_encoded'] == 2
        assert features['duration_normalized'] == 0.2
        assert features['user_follower_norm'] == 0.5
        assert features['content_engagement'] == 0.5


class TestKafkaIntegration:
    
    @pytest.fixture
    def producer(self):
        return Producer({'bootstrap.servers': 'localhost:9092'})
    
    @pytest.fixture
    def consumer(self):
        config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-consumer',
            'auto.offset.reset': 'earliest'
        }
        return Consumer(config)
    
    def test_produce_consume(self, producer, consumer):
        topic = 'test-integration'
        test_data = {'user_id': 1, 'name': 'test'}
        
        producer.produce(
            topic,
            key=b'1',
            value=json.dumps(test_data).encode('utf-8')
        )
        producer.flush()
        
        consumer.subscribe([topic])
        
        msg = None
        for _ in range(10):
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                break
        
        assert msg is not None
        value = json.loads(msg.value().decode('utf-8'))
        assert value['user_id'] == 1
        
        consumer.close()
