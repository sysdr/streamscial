import json
import uuid
import time
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

class HeaderKeys:
    TRACE_ID = "trace-id"
    SPAN_ID = "span-id" 
    PARENT_SPAN_ID = "parent-span-id"
    FEATURE_FLAGS = "feature-flags"
    USER_SEGMENT = "user-segment"
    EXPERIMENT_ID = "experiment-id"
    SERVICE_NAME = "service-name"
    TIMESTAMP = "timestamp"
    USER_ID = "user-id"

@dataclass
class TraceContext:
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    service_name: str = "unknown"
    
    def to_headers(self) -> Dict[str, str]:
        headers = {
            HeaderKeys.TRACE_ID: self.trace_id,
            HeaderKeys.SPAN_ID: self.span_id,
            HeaderKeys.SERVICE_NAME: self.service_name,
            HeaderKeys.TIMESTAMP: str(int(time.time() * 1000))
        }
        if self.parent_span_id:
            headers[HeaderKeys.PARENT_SPAN_ID] = self.parent_span_id
        return headers

@dataclass 
class FeatureFlags:
    enabled_features: Dict[str, bool]
    user_segment: str
    experiment_id: Optional[str] = None
    
    def to_headers(self) -> Dict[str, str]:
        headers = {
            HeaderKeys.FEATURE_FLAGS: json.dumps(self.enabled_features),
            HeaderKeys.USER_SEGMENT: self.user_segment
        }
        if self.experiment_id:
            headers[HeaderKeys.EXPERIMENT_ID] = self.experiment_id
        return headers

class HeaderManager:
    @staticmethod
    def create_trace_context(service_name: str, parent_span_id: Optional[str] = None) -> TraceContext:
        return TraceContext(
            trace_id=str(uuid.uuid4()),
            span_id=str(uuid.uuid4()),
            parent_span_id=parent_span_id,
            service_name=service_name
        )
    
    @staticmethod
    def extract_trace_context(headers: Dict[str, Any]) -> Optional[TraceContext]:
        try:
            trace_id = headers.get(HeaderKeys.TRACE_ID)
            span_id = headers.get(HeaderKeys.SPAN_ID)
            if not trace_id or not span_id:
                return None
            
            return TraceContext(
                trace_id=str(trace_id),
                span_id=str(span_id),
                parent_span_id=headers.get(HeaderKeys.PARENT_SPAN_ID),
                service_name=headers.get(HeaderKeys.SERVICE_NAME, "unknown")
            )
        except Exception:
            return None
    
    @staticmethod
    def extract_feature_flags(headers: Dict[str, Any]) -> Optional[FeatureFlags]:
        try:
            flags_json = headers.get(HeaderKeys.FEATURE_FLAGS)
            user_segment = headers.get(HeaderKeys.USER_SEGMENT)
            
            if not flags_json or not user_segment:
                return None
                
            enabled_features = json.loads(str(flags_json))
            return FeatureFlags(
                enabled_features=enabled_features,
                user_segment=str(user_segment),
                experiment_id=headers.get(HeaderKeys.EXPERIMENT_ID)
            )
        except Exception:
            return None
