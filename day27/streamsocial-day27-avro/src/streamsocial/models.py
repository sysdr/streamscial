from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime
import time

@dataclass
class UserProfile:
    user_id: str
    username: str
    email: str
    created_at: int = None
    profile_version: int = 1
    metadata: Optional[Dict[str, str]] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = int(time.time() * 1000)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class UserInteraction:
    interaction_id: str
    user_id: str
    interaction_type: str
    target_id: str
    content: Optional[str] = None
    timestamp: int = None
    metadata: Optional[Dict[str, str]] = None
    schema_version: int = 1
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = int(time.time() * 1000)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass 
class Location:
    lat: float
    lng: float

@dataclass
class PostEvent:
    post_id: str
    user_id: str
    content: str
    media_urls: List[str] = None
    hashtags: List[str] = None
    mentions: List[str] = None
    created_at: int = None
    location: Optional[Location] = None
    visibility: str = "PUBLIC"
    
    def __post_init__(self):
        if self.media_urls is None:
            self.media_urls = []
        if self.hashtags is None:
            self.hashtags = []
        if self.mentions is None:
            self.mentions = []
        if self.created_at is None:
            self.created_at = int(time.time() * 1000)
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        if self.location:
            result['location'] = asdict(self.location)
        return result
