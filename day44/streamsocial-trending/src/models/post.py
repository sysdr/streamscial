from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
import json

@dataclass
class Post:
    """Represents a social media post"""
    post_id: str
    user_id: str
    content: str
    created_at: datetime
    region: str = "global"
    language: str = "en"
    engagement_score: float = 0.0
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Post':
        data = json.loads(json_str)
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        return cls(**data)
    
    def to_json(self) -> str:
        data = self.__dict__.copy()
        data['created_at'] = data['created_at'].isoformat()
        return json.dumps(data)

@dataclass
class Hashtag:
    """Extracted hashtag from a post"""
    tag: str
    post_id: str
    user_id: str
    timestamp: datetime
    region: str
    language: str
    engagement_score: float
    
    def to_json(self) -> str:
        data = self.__dict__.copy()
        data['timestamp'] = data['timestamp'].isoformat()
        return json.dumps(data)

@dataclass
class WindowedCount:
    """Aggregated counts for a hashtag within a time window"""
    hashtag: str
    window_start: datetime
    window_end: datetime
    mention_count: int = 0
    unique_users: set = field(default_factory=set)
    total_engagement: float = 0.0
    
    def add_mention(self, user_id: str, engagement: float):
        """Incrementally add a mention"""
        self.mention_count += 1
        self.unique_users.add(user_id)
        self.total_engagement += engagement
    
    def to_dict(self) -> dict:
        return {
            'hashtag': self.hashtag,
            'window_start': self.window_start.isoformat(),
            'window_end': self.window_end.isoformat(),
            'mention_count': self.mention_count,
            'unique_user_count': len(self.unique_users),
            'total_engagement': self.total_engagement
        }

@dataclass
class TrendingScore:
    """Trending score for a hashtag"""
    hashtag: str
    score: float
    velocity: float
    mention_count: int
    unique_users: int
    rank: int
    region: str
    timestamp: datetime
    
    def to_json(self) -> str:
        data = self.__dict__.copy()
        data['timestamp'] = data['timestamp'].isoformat()
        return json.dumps(data)
