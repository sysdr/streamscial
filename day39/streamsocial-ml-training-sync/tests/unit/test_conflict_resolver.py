import pytest
from datetime import datetime, timedelta
from src.resolvers.conflict_resolver import ConflictResolver, VersionVector

class TestConflictResolver:
    
    def setup_method(self):
        self.resolver = ConflictResolver()
        self.t1 = datetime.utcnow()
        self.t2 = self.t1 + timedelta(seconds=10)
    
    def test_last_write_wins_incoming_newer(self):
        existing = {'user_id': 1, 'name': 'old', 'updated_at': self.t1}
        incoming = {'user_id': 1, 'name': 'new', 'updated_at': self.t2}
        
        result = self.resolver.last_write_wins(existing, incoming)
        assert result['name'] == 'new'
    
    def test_last_write_wins_existing_newer(self):
        existing = {'user_id': 1, 'name': 'old', 'updated_at': self.t2}
        incoming = {'user_id': 1, 'name': 'new', 'updated_at': self.t1}
        
        result = self.resolver.last_write_wins(existing, incoming)
        assert result['name'] == 'old'
    
    def test_merge_preserves_higher_follower_count(self):
        existing = {
            'user_id': 1, 
            'follower_count': 100, 
            'following_count': 50,
            'updated_at': self.t1
        }
        incoming = {
            'user_id': 1, 
            'follower_count': 95, 
            'following_count': 60,
            'updated_at': self.t2
        }
        
        result = self.resolver.merge_user_profile(existing, incoming)
        assert result['follower_count'] == 100
        assert result['following_count'] == 60
    
    def test_merge_unions_interests(self):
        existing = {
            'user_id': 1, 
            'interests': ['music', 'sports'],
            'updated_at': self.t1
        }
        incoming = {
            'user_id': 1, 
            'interests': ['sports', 'tech'],
            'updated_at': self.t2
        }
        
        result = self.resolver.merge_user_profile(existing, incoming)
        assert set(result['interests']) == {'music', 'sports', 'tech'}
    
    def test_merge_preserves_higher_engagement(self):
        existing = {
            'user_id': 1, 
            'engagement_rate': 0.15,
            'updated_at': self.t1
        }
        incoming = {
            'user_id': 1, 
            'engagement_rate': 0.10,
            'updated_at': self.t2
        }
        
        result = self.resolver.merge_user_profile(existing, incoming)
        assert result['engagement_rate'] == 0.15
    
    def test_content_lww_preserves_engagement(self):
        existing = {
            'content_id': 1, 
            'engagement_score': 80.0,
            'updated_at': self.t1
        }
        incoming = {
            'content_id': 1, 
            'engagement_score': 75.0,
            'updated_at': self.t2
        }
        
        result = self.resolver.merge_content_metadata(existing, incoming)
        assert result['engagement_score'] == 80.0
    
    def test_resolve_routes_correctly(self):
        existing = {'user_id': 1, 'follower_count': 100, 'updated_at': self.t1}
        incoming = {'user_id': 1, 'follower_count': 50, 'updated_at': self.t2}
        
        result = self.resolver.resolve('user_profile', existing, incoming)
        assert result['follower_count'] == 100


class TestVersionVector:
    
    def test_version_vector_comparison_same_source(self):
        v1 = VersionVector('server-1', 10, datetime.utcnow())
        v2 = VersionVector('server-1', 15, datetime.utcnow())
        
        assert v2 > v1
    
    def test_version_vector_comparison_different_source(self):
        t1 = datetime.utcnow()
        t2 = t1 + timedelta(seconds=10)
        
        v1 = VersionVector('server-1', 10, t1)
        v2 = VersionVector('server-2', 5, t2)
        
        assert v2 > v1
    
    def test_version_vector_serialization(self):
        original = VersionVector('server-1', 42, datetime(2025, 5, 15, 10, 30, 0))
        
        data = original.to_dict()
        restored = VersionVector.from_dict(data)
        
        assert restored.source == original.source
        assert restored.sequence == original.sequence
