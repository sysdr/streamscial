"""
Test suite for ACL enforcement
"""
import pytest
import time
from confluent_kafka.admin import ResourceType, AclOperation, ResourcePatternType
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from acl_manager.acl_manager import ACLManager
from services.post_service import PostService
from services.analytics_service import AnalyticsService
from services.moderation_service import ModerationService


class TestACLEnforcement:
    """Test ACL enforcement for all services"""
    
    @pytest.fixture
    def acl_manager(self):
        """Create ACL manager"""
        return ACLManager(
            'localhost:9093',
            'admin',
            'admin-secret'
        )
    
    @pytest.fixture
    def post_service(self):
        """Create post service"""
        return PostService(
            'localhost:9093',
            'post_service',
            'post-secret'
        )
    
    @pytest.fixture
    def analytics_service(self):
        """Create analytics service"""
        return AnalyticsService(
            'localhost:9093',
            'analytics_service',
            'analytics-secret'
        )
    
    @pytest.fixture
    def moderation_service(self):
        """Create moderation service"""
        return ModerationService(
            'localhost:9093',
            'moderation_service',
            'moderation-secret'
        )
    
    def test_post_service_write_access(self, post_service):
        """Test post service can write to posts topics"""
        result = post_service.create_post('user123', 'Test post content')
        assert result['status'] == 'success'
        assert 'post' in result
    
    def test_post_service_denied_analytics(self, post_service):
        """Test post service denied access to analytics"""
        result = post_service.test_unauthorized_access()
        assert result['status'] == 'success'
        assert 'denied' in result['message'].lower()
    
    def test_analytics_service_read_access(self, analytics_service):
        """Test analytics service can read posts"""
        analytics_service.subscribe_to_posts()
        time.sleep(2)
        posts = analytics_service.process_posts(count=5)
        assert isinstance(posts, list)
    
    def test_analytics_service_denied_write(self, analytics_service):
        """Test analytics service denied write to posts"""
        result = analytics_service.test_unauthorized_access()
        assert result['status'] == 'success'
        assert 'denied' in result['message'].lower()
    
    def test_moderation_service_scan(self, moderation_service):
        """Test moderation service can scan posts"""
        moderation_service.subscribe_to_posts()
        time.sleep(2)
        posts = moderation_service.scan_posts(count=5)
        assert isinstance(posts, list)
    
    def test_acl_listing(self, acl_manager):
        """Test ACL listing functionality"""
        acls = acl_manager.list_acls()
        assert isinstance(acls, list)
        assert len(acls) > 0
    
    def test_acl_summary(self, acl_manager):
        """Test ACL summary generation"""
        summary = acl_manager.get_acl_summary()
        assert 'total' in summary
        assert 'by_principal' in summary
        assert summary['total'] > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
