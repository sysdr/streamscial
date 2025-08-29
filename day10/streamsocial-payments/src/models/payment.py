from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
import uuid

class PaymentStatus(str, Enum):
    INITIATED = "initiated"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"

class PaymentEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    amount: float
    currency: str = "USD"
    payment_method: str
    subscription_type: Optional[str] = None
    gift_recipient: Optional[str] = None
    idempotency_key: str
    status: PaymentStatus = PaymentStatus.INITIATED
    external_payment_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def to_kafka_message(self) -> Dict[str, Any]:
        return {
            "event_type": "payment",
            "event_id": self.id,
            "idempotency_key": self.idempotency_key,
            "payload": self.model_dump(mode='json'),
            "timestamp": self.created_at.isoformat()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with serializable datetime"""
        data = self.model_dump()
        data['created_at'] = self.created_at.isoformat()
        data['updated_at'] = self.updated_at.isoformat()
        return data

class PaymentResult(BaseModel):
    success: bool
    payment_id: str
    external_id: Optional[str] = None
    error_message: Optional[str] = None
    retry_after: Optional[int] = None
