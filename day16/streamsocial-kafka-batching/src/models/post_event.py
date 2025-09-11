import json
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
import uuid

@dataclass
class PostEvent:
    user_id: str
    post_id: str
    content: str
    timestamp: int
    region: str
    post_type: str = 'text'
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = int(time.time() * 1000)
        if self.post_id is None:
            self.post_id = str(uuid.uuid4())
            
    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str: str) -> 'PostEvent':
        data = json.loads(json_str)
        return cls(**data)
    
    def get_partition_key(self) -> str:
        return f"{self.region}:{self.user_id}"
