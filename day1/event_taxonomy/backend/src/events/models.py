from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum
import uuid

class EventType(str, Enum):
    USER_REGISTERED = "user_registered"
    POST_CREATED = "post_created"
    COMMENT_ADDED = "comment_added"
    PROFILE_UPDATED = "profile_updated"
    POST_LIKED = "post_liked"
    CONTENT_SHARED = "content_shared"
    STORY_VIEWED = "story_viewed"
    FOLLOW_INITIATED = "follow_initiated"
    CONTENT_MODERATED = "content_moderated"
    SESSION_EXPIRED = "session_expired"

class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_type: EventType
    user_id: str
    data: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True

class UserRegisteredEvent(BaseEvent):
    event_type: EventType = EventType.USER_REGISTERED
    
class PostCreatedEvent(BaseEvent):
    event_type: EventType = EventType.POST_CREATED
    
class CommentAddedEvent(BaseEvent):
    event_type: EventType = EventType.COMMENT_ADDED
