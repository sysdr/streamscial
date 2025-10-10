import json
import logging
from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass
import time

class ErrorType(Enum):
    SKIP = "skip"
    RETRY = "retry" 
    DLQ = "dlq"
    FATAL = "fatal"

@dataclass
class ErrorContext:
    message_key: str
    partition: int
    offset: int
    timestamp: int
    attempt_count: int
    error_type: str
    error_message: str
    original_payload: bytes

class ErrorClassifier:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def classify_error(self, exception: Exception, 
                      context: Dict[str, Any]) -> ErrorType:
        """Classify error based on type and context"""
        
        if isinstance(exception, json.JSONDecodeError):
            # Check if it's a recoverable JSON error
            if self._is_recoverable_json_error(str(exception)):
                return ErrorType.SKIP
            return ErrorType.DLQ
            
        elif isinstance(exception, (KeyError, ValueError)):
            # Missing required fields or invalid values
            if context.get('attempt_count', 0) < 3:
                return ErrorType.RETRY
            return ErrorType.DLQ
            
        elif isinstance(exception, ConnectionError):
            # Network/infrastructure issues - always retry
            return ErrorType.RETRY
            
        elif isinstance(exception, MemoryError):
            # System resource issues - fatal
            return ErrorType.FATAL
            
        else:
            # Unknown errors - send to DLQ for investigation
            return ErrorType.DLQ
    
    def _is_recoverable_json_error(self, error_msg: str) -> bool:
        """Check if JSON error is recoverable (missing optional fields)"""
        recoverable_patterns = [
            "emoji",
            "optional_metadata", 
            "tags",
            "extra_data"
        ]
        return any(pattern in error_msg.lower() for pattern in recoverable_patterns)

    def should_skip_message(self, error_type: ErrorType, 
                          context: Dict[str, Any]) -> bool:
        """Determine if message should be skipped"""
        if error_type == ErrorType.SKIP:
            return True
        
        # Skip if too many retry attempts
        if error_type == ErrorType.RETRY and context.get('attempt_count', 0) > 5:
            return True
            
        return False
