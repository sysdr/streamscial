"""Post data models for StreamSocial."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
import json

class ContentType(Enum):
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    LINK = "link"

class PostStatus(Enum):
    DRAFT = "draft"
    PUBLISHED = "published"
    MODERATED = "moderated"
    DELETED = "deleted"

@dataclass
class User:
    user_id: str
    username: str
    display_name: str
    followers_count: int = 0
    following_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'user_id': self.user_id,
            'username': self.username, 
            'display_name': self.display_name,
            'followers_count': self.followers_count,
            'following_count': self.following_count
        }

@dataclass
class Post:
    post_id: str
    user_id: str
    username: str
    content: str
    content_type: ContentType
    status: PostStatus
    created_at: datetime
    updated_at: datetime
    likes_count: int = 0
    comments_count: int = 0
    shares_count: int = 0
    media_urls: Optional[List[str]] = None
    hashtags: Optional[List[str]] = None
    mentions: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'post_id': self.post_id,
            'user_id': self.user_id,
            'username': self.username,
            'content': self.content,
            'content_type': self.content_type.value,
            'status': self.status.value,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'likes_count': self.likes_count,
            'comments_count': self.comments_count,
            'shares_count': self.shares_count,
            'media_urls': self.media_urls or [],
            'hashtags': self.hashtags or [],
            'mentions': self.mentions or []
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass 
class Notification:
    notification_id: str
    user_id: str
    type: str
    title: str
    message: str
    post_id: Optional[str] = None
    from_user_id: Optional[str] = None
    created_at: Optional[datetime] = None
    read: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'notification_id': self.notification_id,
            'user_id': self.user_id,
            'type': self.type,
            'title': self.title,
            'message': self.message,
            'post_id': self.post_id,
            'from_user_id': self.from_user_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'read': self.read
        }

@dataclass
class AnalyticsEvent:
    event_id: str
    event_type: str
    user_id: str
    post_id: Optional[str]
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'user_id': self.user_id,
            'post_id': self.post_id,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata or {}
        }
