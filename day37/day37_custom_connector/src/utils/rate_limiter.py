"""
Token Bucket Rate Limiter
"""
import time
import threading
from typing import Dict


class TokenBucketRateLimiter:
    """
    Token bucket algorithm for rate limiting
    Refills tokens at specified rate per time window
    """
    
    def __init__(self):
        self.buckets: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        
    def configure(self, platform: str, max_requests: int, window_seconds: int):
        """Configure rate limit for platform"""
        with self.lock:
            self.buckets[platform] = {
                'tokens': max_requests,
                'max_tokens': max_requests,
                'refill_rate': max_requests / window_seconds,  # tokens per second
                'last_refill': time.time(),
            }
            
    def acquire(self, platform: str, tokens_needed: int = 1) -> float:
        """
        Acquire tokens, blocking if insufficient
        Returns wait time in seconds (0 if immediate)
        """
        if platform not in self.buckets:
            return 0.0  # No rate limit configured
            
        with self.lock:
            bucket = self.buckets[platform]
            
            # Refill tokens based on elapsed time
            now = time.time()
            elapsed = now - bucket['last_refill']
            refill_amount = elapsed * bucket['refill_rate']
            
            bucket['tokens'] = min(
                bucket['max_tokens'],
                bucket['tokens'] + refill_amount
            )
            bucket['last_refill'] = now
            
            # Check if enough tokens available
            if bucket['tokens'] >= tokens_needed:
                bucket['tokens'] -= tokens_needed
                return 0.0
            else:
                # Calculate wait time
                tokens_short = tokens_needed - bucket['tokens']
                wait_time = tokens_short / bucket['refill_rate']
                return wait_time
                
    def get_available_tokens(self, platform: str) -> float:
        """Get current available tokens"""
        if platform not in self.buckets:
            return float('inf')
            
        with self.lock:
            return self.buckets[platform]['tokens']
