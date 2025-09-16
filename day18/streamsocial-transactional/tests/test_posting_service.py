"""Tests for StreamSocial posting service."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.streamsocial.services.posting_service import PostingService
from src.streamsocial.models.post import User, ContentType

class TestPostingService:
    
    @pytest.fixture
    def posting_service(self):
        """Create a posting service instance for testing."""
        with patch('src.streamsocial.services.posting_service.create_transactional_producer') as mock_producer:
            mock_producer_instance = Mock()
            mock_producer.return_value = mock_producer_instance
            
            service = PostingService()
            yield service
            service.close()
    
    def test_service_initialization(self, posting_service):
        """Test posting service initializes correctly."""
        assert len(posting_service.users_db) == 5
        assert "user_alice" in posting_service.users_db
        assert posting_service.users_db["user_alice"].username == "alice"
    
    def test_get_users(self, posting_service):
        """Test getting users list."""
        users = posting_service.get_users()
        
        assert len(users) == 5
        assert all(isinstance(user, dict) for user in users)
        assert all('user_id' in user for user in users)
        assert all('username' in user for user in users)
    
    def test_get_user_followers(self, posting_service):
        """Test getting user followers."""
        followers = posting_service.get_user_followers("user_alice", limit=3)
        
        assert len(followers) == 3
        assert "user_alice" not in followers  # User should not follow themselves
        assert all(follower.startswith("user_") for follower in followers)
    
    def test_create_post_success(self, posting_service):
        """Test successful post creation."""
        with patch.object(posting_service.producer, 'atomic_post_creation') as mock_atomic:
            mock_atomic.return_value = True
            
            post_id = posting_service.create_post(
                user_id="user_alice",
                content="Test post content",
                hashtags=["test", "kafka"]
            )
            
            assert post_id is not None
            assert post_id.startswith("post_")
            mock_atomic.assert_called_once()
    
    def test_create_post_invalid_user(self, posting_service):
        """Test post creation with invalid user."""
        post_id = posting_service.create_post(
            user_id="invalid_user",
            content="Test content"
        )
        
        assert post_id is None
    
    def test_create_post_atomic_failure(self, posting_service):
        """Test post creation when atomic operation fails."""
        with patch.object(posting_service.producer, 'atomic_post_creation') as mock_atomic:
            mock_atomic.return_value = False
            
            post_id = posting_service.create_post(
                user_id="user_alice",
                content="Test content"
            )
            
            assert post_id is None
    
    def test_follower_timeline_sync_success(self, posting_service):
        """Test successful follower timeline sync."""
        with patch.object(posting_service.producer, 'atomic_multi_user_timeline_update') as mock_sync:
            mock_sync.return_value = True
            
            target_followers = ["user_bob", "user_carol"]
            
            result = posting_service.create_follower_timeline_sync(
                user_id="user_alice",
                content="Sync test",
                target_followers=target_followers
            )
            
            assert result is True
            mock_sync.assert_called_once()
    
    def test_get_service_metrics(self, posting_service):
        """Test service metrics collection."""
        with patch.object(posting_service.producer, 'get_transaction_metrics') as mock_metrics:
            mock_metrics.return_value = {"test": "metrics"}
            
            metrics = posting_service.get_service_metrics()
            
            assert 'total_users' in metrics
            assert 'producer_metrics' in metrics
            assert 'service_status' in metrics
            
            assert metrics['total_users'] == 5
            assert metrics['service_status'] == 'active'

if __name__ == "__main__":
    pytest.main([__file__])
