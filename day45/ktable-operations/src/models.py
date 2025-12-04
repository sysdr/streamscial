from enum import Enum
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class ActionType(str, Enum):
    LIKE = "like"
    COMMENT = "comment"
    SHARE = "share"

class UserAction(BaseModel):
    user_id: str
    action_type: ActionType
    post_id: str
    timestamp: datetime
    
class ReputationScore(BaseModel):
    user_id: str
    score: int
    like_count: int
    comment_count: int
    share_count: int
    last_updated: datetime

class ReputationTier(str, Enum):
    BRONZE = "bronze"      # 0-99
    SILVER = "silver"      # 100-499
    GOLD = "gold"          # 500-1999
    PLATINUM = "platinum"  # 2000+

