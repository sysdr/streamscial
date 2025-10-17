import pytest
import time
import json
from src.streamsocial.models import UserProfile, UserInteraction, PostEvent
from src.registry.schema_manager import StreamSocialSchemaManager
from confluent_kafka.serialization import SerializationContext, MessageField

class TestAvroSerialization:
    
    def setup_method(self):
        self.schema_manager = StreamSocialSchemaManager("http://localhost:8081")
        
    def test_user_profile_schema_loading(self):
        """Test loading user profile schema"""
        schema = self.schema_manager.load_schema('user_profile')
        assert schema is not None
        assert schema.name == 'UserProfile'
        
    def test_user_interaction_schema_loading(self):
        """Test loading user interaction schema"""
        schema = self.schema_manager.load_schema('user_interaction')
        assert schema is not None
        assert schema.name == 'UserInteraction'
        
    def test_post_event_schema_loading(self):
        """Test loading post event schema"""
        schema = self.schema_manager.load_schema('post_event')
        assert schema is not None
        assert schema.name == 'PostEvent'
        
    def test_model_serialization(self):
        """Test model to dict conversion"""
        profile = UserProfile("user123", "johndoe", "john@example.com")
        data = profile.to_dict()
        
        assert data['user_id'] == "user123"
        assert data['username'] == "johndoe"
        assert data['email'] == "john@example.com"
        assert 'created_at' in data
        assert data['profile_version'] == 1
        
    def test_interaction_model(self):
        """Test interaction model"""
        interaction = UserInteraction("int456", "user123", "LIKE", "post789")
        data = interaction.to_dict()
        
        assert data['interaction_type'] == "LIKE"
        assert data['user_id'] == "user123"
        assert data['target_id'] == "post789"
        assert 'timestamp' in data
        
    def test_post_model_with_metadata(self):
        """Test post model with complex data"""
        post = PostEvent(
            "post789", "user123", "Hello StreamSocial! #kafka #avro",
            hashtags=["kafka", "avro"], 
            mentions=["@alice"]
        )
        data = post.to_dict()
        
        assert len(data['hashtags']) == 2
        assert "kafka" in data['hashtags']
        assert data['visibility'] == "PUBLIC"
        assert 'created_at' in data

if __name__ == "__main__":
    pytest.main([__file__])
