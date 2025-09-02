import time
import random
import math
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class BackoffState:
    attempt: int = 0
    last_attempt_time: float = 0
    next_retry_time: float = 0
    total_delay: float = 0

class ExponentialBackoffCalculator:
    """Calculates exponential backoff delays with jitter"""
    
    def __init__(self, config: Dict[str, Any]):
        self.base_delay = config['base_delay'] / 1000.0  # Convert to seconds
        self.max_delay = config['max_delay'] / 1000.0
        self.multiplier = config['backoff_multiplier']
        self.jitter_enabled = config['jitter_enabled']
        self.jitter_factor = config['jitter_factor']
        
    def calculate_delay(self, attempt: int) -> float:
        """Calculate backoff delay for given attempt number"""
        # Basic exponential backoff
        delay = self.base_delay * (self.multiplier ** attempt)
        
        # Apply maximum delay cap
        delay = min(delay, self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter_enabled:
            jitter_range = delay * self.jitter_factor
            jitter = random.uniform(-jitter_range, jitter_range)
            delay = max(0, delay + jitter)
        
        return delay
    
    def should_retry(self, state: BackoffState, max_retries: int) -> bool:
        """Determine if we should attempt retry based on current state"""
        if state.attempt >= max_retries:
            return False
        
        current_time = time.time()
        return current_time >= state.next_retry_time
    
    def update_state(self, state: BackoffState) -> BackoffState:
        """Update backoff state for next attempt"""
        current_time = time.time()
        delay = self.calculate_delay(state.attempt)
        
        return BackoffState(
            attempt=state.attempt + 1,
            last_attempt_time=current_time,
            next_retry_time=current_time + delay,
            total_delay=state.total_delay + delay
        )
