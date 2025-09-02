import logging
from enum import Enum
from kafka.errors import KafkaError, KafkaTimeoutError, RequestTimedOutError
from kafka.errors import AuthenticationFailedError, TopicAuthorizationFailedError

logger = logging.getLogger(__name__)

class FailureType(Enum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"

class FailureClassifier:
    """Classifies Kafka failures as transient or permanent"""
    
    TRANSIENT_ERRORS = {
        KafkaTimeoutError,
        RequestTimedOutError,
        ConnectionError,
        OSError
    }
    
    PERMANENT_ERRORS = {
        AuthenticationFailedError,
        TopicAuthorizationFailedError,
        ValueError,
        TypeError
    }
    
    @classmethod
    def classify(cls, exception: Exception) -> FailureType:
        """Classify exception as transient, permanent, or unknown"""
        exception_type = type(exception)
        
        if exception_type in cls.TRANSIENT_ERRORS:
            logger.debug(f"Classified {exception_type.__name__} as TRANSIENT")
            return FailureType.TRANSIENT
        
        if exception_type in cls.PERMANENT_ERRORS:
            logger.debug(f"Classified {exception_type.__name__} as PERMANENT")
            return FailureType.PERMANENT
        
        # Check error message for additional clues
        error_message = str(exception).lower()
        
        if any(keyword in error_message for keyword in ['timeout', 'connection', 'network']):
            return FailureType.TRANSIENT
        
        if any(keyword in error_message for keyword in ['unauthorized', 'authentication', 'permission']):
            return FailureType.PERMANENT
        
        logger.warning(f"Unknown failure type for {exception_type.__name__}: {exception}")
        return FailureType.UNKNOWN
