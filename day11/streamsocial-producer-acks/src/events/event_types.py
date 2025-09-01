from enum import Enum
from dataclasses import dataclass, asdict
import json
import time
from typing import Dict, Any

class EventType(Enum):
    # Critical Events
    USER_REGISTRATION = "user_registration"
    PAYMENT_PROCESSING = "payment_processing"
    PREMIUM_SUBSCRIPTION = "premium_subscription"
    
    # Social Events
    POST_CREATION = "post_creation"
    COMMENT_ADDED = "comment_added"
    FRIEND_REQUEST = "friend_request"
    PROFILE_UPDATE = "profile_update"
    
    # Analytics Events
    PAGE_VIEW = "page_view"
    CLICK_TRACKING = "click_tracking"
    SCROLL_BEHAVIOR = "scroll_behavior"
    AD_IMPRESSION = "ad_impression"

@dataclass
class StreamSocialEvent:
    event_type: str
    user_id: str
    timestamp: float
    data: Dict[str, Any]
    session_id: str
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @classmethod
    def create_critical_event(cls, event_type: EventType, user_id: str, data: Dict[str, Any]):
        return cls(
            event_type=event_type.value,
            user_id=user_id,
            timestamp=time.time(),
            data=data,
            session_id=f"session_{int(time.time())}"
        )
    
    @classmethod
    def create_social_event(cls, event_type: EventType, user_id: str, data: Dict[str, Any]):
        return cls(
            event_type=event_type.value,
            user_id=user_id,
            timestamp=time.time(),
            data=data,
            session_id=f"session_{int(time.time())}"
        )
    
    @classmethod
    def create_analytics_event(cls, event_type: EventType, user_id: str, data: Dict[str, Any]):
        return cls(
            event_type=event_type.value,
            user_id=user_id,
            timestamp=time.time(),
            data=data,
            session_id=f"session_{int(time.time())}"
        )
