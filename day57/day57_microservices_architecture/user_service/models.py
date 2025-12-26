"""User service database models"""
from dataclasses import dataclass
from typing import Optional

@dataclass
class User:
    user_id: str
    email: str
    username: str
    created_at: str
    bio: Optional[str] = None
