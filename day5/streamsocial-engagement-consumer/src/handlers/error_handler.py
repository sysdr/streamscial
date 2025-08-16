import structlog
import time
from typing import Optional
from kafka import TopicPartition
from kafka.consumer.fetcher import ConsumerRecord

logger = structlog.get_logger()

class ErrorHandler:
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.retry_counts = {}
        self.dead_letter_messages = []
    
    def handle_processing_error(self, message: ConsumerRecord, error: Exception) -> bool:
        """
        Handle processing errors with exponential backoff
        Returns True if message should be retried, False if should be discarded
        """
        key = f"{message.topic}:{message.partition}:{message.offset}"
        retry_count = self.retry_counts.get(key, 0)
        
        if retry_count < self.max_retries:
            self.retry_counts[key] = retry_count + 1
            delay = self.base_delay * (2 ** retry_count)
            
            logger.warning(
                "Processing error, retrying",
                error=str(error),
                retry_count=retry_count + 1,
                delay_seconds=delay,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset
            )
            
            time.sleep(delay)
            return True
        else:
            logger.error(
                "Max retries exceeded, sending to dead letter queue",
                error=str(error),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset
            )
            
            self.dead_letter_messages.append({
                'original_message': message.value,
                'error': str(error),
                'timestamp': time.time(),
                'retry_count': retry_count
            })
            
            # Remove from retry tracking
            if key in self.retry_counts:
                del self.retry_counts[key]
            
            return False
    
    def get_dead_letter_messages(self):
        """Get all dead letter messages"""
        return self.dead_letter_messages.copy()
    
    def clear_dead_letter_messages(self):
        """Clear dead letter messages"""
        self.dead_letter_messages.clear()
