from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from datetime import datetime
import json

@dataclass
class UserProfile:
    user_id: str
    age: int
    city: str
    interests: list
    tier: str
    joined_date: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class UserAction:
    user_id: str
    action_type: str
    target_id: str
    timestamp: int
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class EnrichedAction:
    user_id: str
    action_type: str
    target_id: str
    timestamp: int
    user_age: Optional[int] = None
    user_city: Optional[str] = None
    user_tier: Optional[str] = None
    user_interests: Optional[list] = None
    enrichment_latency_ms: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
