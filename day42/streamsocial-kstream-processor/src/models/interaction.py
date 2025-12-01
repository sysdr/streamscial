from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime
import json

@dataclass
class InteractionEvent:
    """Raw user interaction event"""
    user_id: str
    post_id: str
    interaction_type: str  # LIKE, SHARE, COMMENT, VIEW
    timestamp: float
    content_id: Optional[str] = None
    metadata: Optional[dict] = None
    
    def to_json(self):
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(data):
        if isinstance(data, str):
            data = json.loads(data)
        return InteractionEvent(**data)
    
    def is_valid(self):
        """Validate interaction event"""
        if not self.user_id or not self.post_id:
            return False
        if self.interaction_type not in ['LIKE', 'SHARE', 'COMMENT', 'VIEW']:
            return False
        if self.timestamp <= 0:
            return False
        return True

@dataclass
class RankingSignal:
    """Processed feed ranking signal"""
    user_id: str
    post_id: str
    interaction_type: str
    engagement_score: float
    timestamp: float
    recency_factor: float
    user_reputation: float
    base_weight: float
    processing_time_ms: float
    
    def to_json(self):
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(data):
        if isinstance(data, str):
            data = json.loads(data)
        return RankingSignal(**data)
