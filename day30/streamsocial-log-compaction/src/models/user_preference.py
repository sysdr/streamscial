from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any, Optional
import json

@dataclass
class NotificationSettings:
    email: bool = True
    push: bool = True
    sms: bool = False

@dataclass
class PrivacySettings:
    profile_visibility: str = "public"  # public, friends, private
    show_activity: bool = True
    allow_tagging: bool = True

@dataclass
class UserPreference:
    user_id: str
    theme: str = "light"  # light, dark, auto
    language: str = "en"
    timezone: str = "UTC"
    notifications: NotificationSettings = None
    privacy: PrivacySettings = None
    updated_at: str = None
    version: int = 1

    def __post_init__(self):
        if self.notifications is None:
            self.notifications = NotificationSettings()
        if self.privacy is None:
            self.privacy = PrivacySettings()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserPreference':
        if 'notifications' in data and isinstance(data['notifications'], dict):
            data['notifications'] = NotificationSettings(**data['notifications'])
        if 'privacy' in data and isinstance(data['privacy'], dict):
            data['privacy'] = PrivacySettings(**data['privacy'])
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> 'UserPreference':
        return cls.from_dict(json.loads(json_str))

    def update_version(self) -> 'UserPreference':
        self.version += 1
        self.updated_at = datetime.utcnow().isoformat()
        return self
