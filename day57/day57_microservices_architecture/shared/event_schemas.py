"""Domain event schemas"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict
import json
import uuid

@dataclass
class DomainEvent:
    event_type: str
    event_id: str
    aggregate_id: str
    timestamp: str
    data: Dict[str, Any]
    version: int = 1
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DomainEvent':
        data = json.loads(json_str)
        return cls(**data)

class EventFactory:
    @staticmethod
    def user_registered(user_id: str, email: str, username: str) -> DomainEvent:
        return DomainEvent(
            event_type="UserRegistered",
            event_id=str(uuid.uuid4()),
            aggregate_id=user_id,
            timestamp=datetime.utcnow().isoformat(),
            data={"user_id": user_id, "email": email, "username": username}
        )
    
    @staticmethod
    def post_created(post_id: str, user_id: str, content: str) -> DomainEvent:
        return DomainEvent(
            event_type="PostCreated",
            event_id=str(uuid.uuid4()),
            aggregate_id=post_id,
            timestamp=datetime.utcnow().isoformat(),
            data={"post_id": post_id, "user_id": user_id, "content": content}
        )
    
    @staticmethod
    def post_liked(post_id: str, user_id: str) -> DomainEvent:
        return DomainEvent(
            event_type="PostLiked",
            event_id=str(uuid.uuid4()),
            aggregate_id=post_id,
            timestamp=datetime.utcnow().isoformat(),
            data={"post_id": post_id, "user_id": user_id}
        )
    
    @staticmethod
    def notification_sent(notification_id: str, user_id: str, message: str) -> DomainEvent:
        return DomainEvent(
            event_type="NotificationSent",
            event_id=str(uuid.uuid4()),
            aggregate_id=notification_id,
            timestamp=datetime.utcnow().isoformat(),
            data={"notification_id": notification_id, "user_id": user_id, "message": message}
        )
