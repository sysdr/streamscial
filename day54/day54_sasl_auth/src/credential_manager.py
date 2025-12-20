"""Credential management system with encryption and rotation support."""

import json
import hashlib
import secrets
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from pathlib import Path


class CredentialManager:
    """Manages SASL credentials with encryption and rotation."""
    
    def __init__(self, storage_path: str = "data/credentials"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.credentials_file = self.storage_path / "credentials.enc"
        self.key_file = self.storage_path / "encryption.key"
        
        # Initialize encryption key
        if not self.key_file.exists():
            key = Fernet.generate_key()
            self.key_file.write_bytes(key)
        
        self.cipher = Fernet(self.key_file.read_bytes())
        self.credentials: Dict = self._load_credentials()
        
    def _load_credentials(self) -> Dict:
        """Load encrypted credentials from storage."""
        if not self.credentials_file.exists():
            return {
                "users": {},
                "rotation_schedule": {},
                "audit_log": []
            }
        
        encrypted_data = self.credentials_file.read_bytes()
        decrypted_data = self.cipher.decrypt(encrypted_data)
        return json.loads(decrypted_data)
    
    def _save_credentials(self):
        """Save credentials with encryption."""
        data = json.dumps(self.credentials, indent=2)
        encrypted_data = self.cipher.encrypt(data.encode())
        self.credentials_file.write_bytes(encrypted_data)
    
    def create_user(self, username: str, password: str, mechanism: str = "SCRAM-SHA-512") -> Dict:
        """Create new user with specified mechanism."""
        if username in self.credentials["users"]:
            raise ValueError(f"User {username} already exists")
        
        user_data = {
            "username": username,
            "password_hash": hashlib.sha512(password.encode()).hexdigest(),
            "mechanism": mechanism,
            "created_at": datetime.now().isoformat(),
            "last_rotated": datetime.now().isoformat(),
            "rotation_days": 90,
            "enabled": True
        }
        
        self.credentials["users"][username] = user_data
        self._log_audit("USER_CREATED", username, {"mechanism": mechanism})
        self._save_credentials()
        
        return user_data
    
    def delete_user(self, username: str) -> bool:
        """Delete user and revoke access."""
        if username not in self.credentials["users"]:
            return False
        
        del self.credentials["users"][username]
        self._log_audit("USER_DELETED", username)
        self._save_credentials()
        return True
    
    def rotate_password(self, username: str, new_password: str) -> bool:
        """Rotate user password."""
        if username not in self.credentials["users"]:
            return False
        
        old_hash = self.credentials["users"][username]["password_hash"]
        self.credentials["users"][username]["password_hash"] = hashlib.sha512(
            new_password.encode()
        ).hexdigest()
        self.credentials["users"][username]["last_rotated"] = datetime.now().isoformat()
        
        self._log_audit("PASSWORD_ROTATED", username, {
            "old_hash": old_hash[:16],
            "new_hash": self.credentials["users"][username]["password_hash"][:16]
        })
        self._save_credentials()
        return True
    
    def get_users_needing_rotation(self) -> List[Dict]:
        """Get users whose passwords need rotation."""
        users_to_rotate = []
        current_time = datetime.now()
        
        for username, user_data in self.credentials["users"].items():
            last_rotated = datetime.fromisoformat(user_data["last_rotated"])
            rotation_days = user_data.get("rotation_days", 90)
            
            if (current_time - last_rotated).days >= rotation_days:
                users_to_rotate.append({
                    "username": username,
                    "days_overdue": (current_time - last_rotated).days - rotation_days,
                    "mechanism": user_data["mechanism"]
                })
        
        return users_to_rotate
    
    def validate_credentials(self, username: str, password: str) -> bool:
        """Validate user credentials."""
        if username not in self.credentials["users"]:
            self._log_audit("AUTH_FAILED", username, {"reason": "user_not_found"})
            return False
        
        user_data = self.credentials["users"][username]
        if not user_data.get("enabled", True):
            self._log_audit("AUTH_FAILED", username, {"reason": "user_disabled"})
            return False
        
        password_hash = hashlib.sha512(password.encode()).hexdigest()
        is_valid = password_hash == user_data["password_hash"]
        
        if is_valid:
            self._log_audit("AUTH_SUCCESS", username)
        else:
            self._log_audit("AUTH_FAILED", username, {"reason": "invalid_password"})
        
        return is_valid
    
    def _log_audit(self, action: str, username: str, metadata: Optional[Dict] = None):
        """Log authentication and credential events."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "username": username,
            "metadata": metadata or {}
        }
        
        self.credentials["audit_log"].append(log_entry)
        
        # Keep only last 10000 entries
        if len(self.credentials["audit_log"]) > 10000:
            self.credentials["audit_log"] = self.credentials["audit_log"][-10000:]
    
    def get_audit_log(self, limit: int = 100) -> List[Dict]:
        """Get recent audit log entries."""
        return self.credentials["audit_log"][-limit:]
    
    def get_all_users(self) -> List[Dict]:
        """Get all users (without sensitive data)."""
        return [
            {
                "username": username,
                "mechanism": user_data["mechanism"],
                "created_at": user_data["created_at"],
                "last_rotated": user_data["last_rotated"],
                "enabled": user_data.get("enabled", True)
            }
            for username, user_data in self.credentials["users"].items()
        ]
