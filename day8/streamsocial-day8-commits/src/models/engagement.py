from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime
import json

@dataclass
class EngagementEvent:
    user_id: str
    content_id: str
    engagement_type: str  # like, share, comment, follow
    timestamp: datetime
    metadata: Dict[str, Any]
    event_id: Optional[str] = None
    processed: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'user_id': self.user_id,
            'content_id': self.content_id,
            'engagement_type': self.engagement_type,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata,
            'event_id': self.event_id,
            'processed': self.processed
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EngagementEvent':
        return cls(
            user_id=data['user_id'],
            content_id=data['content_id'],
            engagement_type=data['engagement_type'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            metadata=data['metadata'],
            event_id=data.get('event_id'),
            processed=data.get('processed', False)
        )
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'EngagementEvent':
        return cls.from_dict(json.loads(json_str))
