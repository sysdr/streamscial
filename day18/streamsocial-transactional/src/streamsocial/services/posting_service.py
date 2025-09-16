"""StreamSocial posting service with transactional operations."""

import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import asdict

from src.streamsocial.models.post import Post, User, ContentType, PostStatus
from src.kafka_client.producers.transactional_producer import create_transactional_producer, TransactionError

logger = logging.getLogger(__name__)

class PostingService:
    """Service for handling StreamSocial post creation with atomic guarantees."""
    
    def __init__(self):
        self.producer = create_transactional_producer("posting-service")
        self.users_db = self._initialize_mock_users()
        logger.info("PostingService initialized with transactional producer")
    
    def _initialize_mock_users(self) -> Dict[str, User]:
        """Initialize mock users for demonstration."""
        users = {}
        user_data = [
            ("alice", "Alice Johnson", 1250),
            ("bob", "Bob Wilson", 890),
            ("carol", "Carol Davis", 2340),
            ("david", "David Brown", 456),
            ("eve", "Eve Miller", 3200)
        ]
        
        for username, display_name, followers in user_data:
            user_id = f"user_{username}"
            users[user_id] = User(
                user_id=user_id,
                username=username,
                display_name=display_name,
                followers_count=followers,
                following_count=len(user_data) - 1
            )
        
        return users
    
    def get_user_followers(self, user_id: str, limit: int = 100) -> List[str]:
        """Get list of follower IDs for a user (mock implementation)."""
        # In real implementation, this would query a followers database
        all_users = list(self.users_db.keys())
        followers = [uid for uid in all_users if uid != user_id]
        return followers[:limit]
    
    def create_post(self, user_id: str, content: str, content_type: str = "text", 
                   media_urls: Optional[List[str]] = None,
                   hashtags: Optional[List[str]] = None,
                   mentions: Optional[List[str]] = None) -> Optional[str]:
        """Create a new post with atomic guarantees across all topics."""
        
        if user_id not in self.users_db:
            logger.error(f"User not found: {user_id}")
            return None
        
        user = self.users_db[user_id]
        post_id = f"post_{uuid.uuid4().hex[:12]}"
        
        try:
            # Create post object
            post = Post(
                post_id=post_id,
                user_id=user_id,
                username=user.username,
                content=content,
                content_type=ContentType(content_type),
                status=PostStatus.PUBLISHED,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                media_urls=media_urls,
                hashtags=hashtags,
                mentions=mentions
            )
            
            # Get followers for notifications
            followers = self.get_user_followers(user_id, limit=50)
            
            # Perform atomic operation
            success = self.producer.atomic_post_creation(post, followers)
            
            if success:
                logger.info(f"Post created successfully: {post_id} by {user.username}")
                return post_id
            else:
                logger.error(f"Failed to create post atomically: {post_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating post: {e}")
            return None
    
    def create_follower_timeline_sync(self, user_id: str, content: str, 
                                    target_followers: List[str]) -> bool:
        """Create post and sync to specific followers' timelines atomically."""
        
        if user_id not in self.users_db:
            logger.error(f"User not found: {user_id}")
            return False
        
        user = self.users_db[user_id]
        post_id = f"sync_post_{uuid.uuid4().hex[:12]}"
        
        try:
            post = Post(
                post_id=post_id,
                user_id=user_id,
                username=user.username,
                content=content,
                content_type=ContentType.TEXT,
                status=PostStatus.PUBLISHED,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            # Atomic multi-user timeline update
            success = self.producer.atomic_multi_user_timeline_update(post, target_followers)
            
            if success:
                logger.info(f"Follower timeline sync successful for post: {post_id}")
                return True
            else:
                logger.error(f"Follower timeline sync failed for post: {post_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error in follower timeline sync: {e}")
            return False
    
    def get_users(self) -> List[Dict[str, Any]]:
        """Get all users for UI display."""
        return [user.to_dict() for user in self.users_db.values()]
    
    def get_service_metrics(self) -> Dict[str, Any]:
        """Get service metrics including producer metrics."""
        return {
            'total_users': len(self.users_db),
            'producer_metrics': self.producer.get_transaction_metrics(),
            'service_status': 'active'
        }
    
    def close(self):
        """Close service and cleanup resources."""
        self.producer.close()
        logger.info("PostingService closed")
