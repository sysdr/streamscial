from pydantic import BaseModel, validator
from typing import Optional, Literal
from datetime import datetime
import json

class EngagementEvent(BaseModel):
    user_id: str
    post_id: str
    action_type: Literal['like', 'share', 'comment']
    timestamp: datetime
    content: Optional[str] = None  # For comments
    metadata: Optional[dict] = {}
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v
    
    @classmethod
    def from_kafka_message(cls, message_value: bytes) -> 'EngagementEvent':
        """Deserialize Kafka message to EngagementEvent"""
        try:
            data = json.loads(message_value.decode('utf-8'))
            return cls(**data)
        except Exception as e:
            raise ValueError(f"Failed to deserialize message: {e}")

class EngagementStats(BaseModel):
    post_id: str
    likes: int = 0
    shares: int = 0
    comments: int = 0
    last_updated: datetime
    
    def update(self, action_type: str):
        """Update stats based on action type"""
        if action_type == 'like':
            self.likes += 1
        elif action_type == 'share':
            self.shares += 1
        elif action_type == 'comment':
            self.comments += 1
        self.last_updated = datetime.now()
