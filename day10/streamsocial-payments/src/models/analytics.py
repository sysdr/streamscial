from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any

class AnalyticsEvent(BaseModel):
    event_type: str
    user_id: str
    amount: float
    payment_method: str
    success: bool
    processing_time_ms: float
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_message(self) -> Dict[str, Any]:
        return {
            "event_type": "analytics",
            "payload": self.model_dump(),
            "timestamp": self.timestamp.isoformat()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with serializable datetime"""
        data = self.model_dump()
        data['timestamp'] = self.timestamp.isoformat()
        return data
