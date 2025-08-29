from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any

class UserUpdate(BaseModel):
    user_id: str
    subscription_type: Optional[str] = None
    subscription_status: Optional[str] = None
    balance: Optional[float] = None
    payment_history: Dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_message(self) -> Dict[str, Any]:
        return {
            "event_type": "user_update",
            "user_id": self.user_id,
            "payload": self.model_dump(),
            "timestamp": self.updated_at.isoformat()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with serializable datetime"""
        data = self.model_dump()
        data['updated_at'] = self.updated_at.isoformat()
        return data
