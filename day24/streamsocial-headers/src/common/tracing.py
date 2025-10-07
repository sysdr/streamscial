import time
import threading
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from contextlib import contextmanager

@dataclass
class Span:
    trace_id: str
    span_id: str
    operation_name: str
    service_name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    parent_span_id: Optional[str] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: list = field(default_factory=list)
    
    def finish(self):
        self.end_time = time.time()
    
    def duration_ms(self) -> float:
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000
    
    def add_tag(self, key: str, value: Any):
        self.tags[key] = value
    
    def log(self, message: str, **kwargs):
        self.logs.append({
            'timestamp': time.time(),
            'message': message,
            **kwargs
        })

class TracingContext:
    _local = threading.local()
    
    @classmethod
    def set_current_span(cls, span: Optional[Span]):
        cls._local.current_span = span
    
    @classmethod
    def get_current_span(cls) -> Optional[Span]:
        return getattr(cls._local, 'current_span', None)
    
    @classmethod
    @contextmanager
    def span(cls, operation_name: str, service_name: str, 
             trace_id: Optional[str] = None, parent_span_id: Optional[str] = None):
        from .headers import HeaderManager
        
        current = cls.get_current_span()
        if current and not trace_id:
            trace_id = current.trace_id
            parent_span_id = current.span_id
        
        if not trace_id:
            trace_context = HeaderManager.create_trace_context(service_name)
            trace_id = trace_context.trace_id
        
        span = Span(
            trace_id=trace_id,
            span_id=HeaderManager.create_trace_context(service_name).span_id,
            operation_name=operation_name,
            service_name=service_name,
            parent_span_id=parent_span_id
        )
        
        cls.set_current_span(span)
        try:
            yield span
        finally:
            span.finish()
            global_tracer.record_span(span)
            cls.set_current_span(current)

# Global tracer for collecting spans
class InMemoryTracer:
    def __init__(self):
        self.spans = []
        self._lock = threading.Lock()
    
    def record_span(self, span: Span):
        with self._lock:
            self.spans.append(span)
    
    def get_traces(self, trace_id: Optional[str] = None) -> list:
        with self._lock:
            if trace_id:
                return [s for s in self.spans if s.trace_id == trace_id]
            return self.spans.copy()
    
    def clear(self):
        with self._lock:
            self.spans.clear()

global_tracer = InMemoryTracer()
