import time
import threading
from enum import Enum
from typing import Dict, Any, Callable
from dataclasses import dataclass

class CircuitBreakerState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery

@dataclass
class CircuitBreakerMetrics:
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0
    last_success_time: float = 0
    state_change_time: float = 0

class CircuitBreaker:
    """Circuit breaker implementation for producer resilience"""
    
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self.state = CircuitBreakerState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        self.lock = threading.RLock()
        
    def __call__(self, func: Callable) -> Callable:
        """Decorator to wrap functions with circuit breaker"""
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self.lock:
            current_time = time.time()
            
            # Handle open circuit
            if self.state == CircuitBreakerState.OPEN:
                if current_time - self.metrics.state_change_time >= self.recovery_timeout:
                    self._half_open_circuit(current_time)
                else:
                    raise CircuitBreakerOpenException(f"Circuit breaker {self.name} is OPEN")
        
        # Execute the function
        try:
            result = func(*args, **kwargs)
            self._record_success(current_time)
            return result
        except Exception as e:
            self._record_failure(current_time)
            raise e
    
    def _open_circuit(self, current_time: float):
        """Transition to OPEN state"""
        self.state = CircuitBreakerState.OPEN
        self.metrics.state_change_time = current_time
        print(f"🔴 Circuit breaker {self.name} OPENED at {time.strftime('%H:%M:%S')}")
    
    def _half_open_circuit(self, current_time: float):
        """Transition to HALF_OPEN state"""
        self.state = CircuitBreakerState.HALF_OPEN
        self.metrics.state_change_time = current_time
        self.metrics.failure_count = 0  # Reset for testing
        print(f"🟡 Circuit breaker {self.name} HALF-OPEN at {time.strftime('%H:%M:%S')}")
    
    def _close_circuit(self, current_time: float):
        """Transition to CLOSED state"""
        self.state = CircuitBreakerState.CLOSED
        self.metrics.state_change_time = current_time
        self.metrics.failure_count = 0
        print(f"🟢 Circuit breaker {self.name} CLOSED at {time.strftime('%H:%M:%S')}")
    
    def _record_success(self, current_time: float):
        """Record successful execution"""
        with self.lock:
            self.metrics.success_count += 1
            self.metrics.last_success_time = current_time
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self._close_circuit(current_time)
    
    def _record_failure(self, current_time: float):
        """Record failed execution"""
        with self.lock:
            self.metrics.failure_count += 1
            self.metrics.last_failure_time = current_time
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self._open_circuit(current_time)
            elif self.state == CircuitBreakerState.CLOSED and self.metrics.failure_count >= self.failure_threshold:
                self._open_circuit(current_time)

class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open"""
    pass
