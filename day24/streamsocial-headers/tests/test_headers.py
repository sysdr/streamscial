import pytest
import json
import time
from unittest.mock import Mock, patch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from common.headers import HeaderManager, TraceContext, FeatureFlags, HeaderKeys
from common.tracing import TracingContext, global_tracer

class TestHeaders:
    
    def test_trace_context_creation(self):
        trace = HeaderManager.create_trace_context("test-service")
        assert trace.service_name == "test-service"
        assert trace.trace_id is not None
        assert trace.span_id is not None
    
    def test_trace_context_headers(self):
        trace = HeaderManager.create_trace_context("test-service", "parent-123")
        headers = trace.to_headers()
        
        assert headers[HeaderKeys.TRACE_ID] == trace.trace_id
        assert headers[HeaderKeys.SPAN_ID] == trace.span_id
        assert headers[HeaderKeys.PARENT_SPAN_ID] == "parent-123"
        assert headers[HeaderKeys.SERVICE_NAME] == "test-service"
        assert HeaderKeys.TIMESTAMP in headers
    
    def test_feature_flags_creation(self):
        flags = FeatureFlags(
            enabled_features={"feature1": True, "feature2": False},
            user_segment="premium",
            experiment_id="exp-123"
        )
        
        headers = flags.to_headers()
        assert headers[HeaderKeys.USER_SEGMENT] == "premium"
        assert headers[HeaderKeys.EXPERIMENT_ID] == "exp-123"
        
        parsed_flags = json.loads(headers[HeaderKeys.FEATURE_FLAGS])
        assert parsed_flags["feature1"] is True
        assert parsed_flags["feature2"] is False
    
    def test_header_extraction(self):
        # Test trace context extraction
        headers = {
            HeaderKeys.TRACE_ID: "trace-123",
            HeaderKeys.SPAN_ID: "span-456",
            HeaderKeys.SERVICE_NAME: "test-service"
        }
        
        trace = HeaderManager.extract_trace_context(headers)
        assert trace.trace_id == "trace-123"
        assert trace.span_id == "span-456"
        assert trace.service_name == "test-service"
    
    def test_tracing_context(self):
        global_tracer.clear()
        
        with TracingContext.span("test-operation", "test-service") as span:
            assert span.operation_name == "test-operation"
            assert span.service_name == "test-service"
            assert TracingContext.get_current_span() == span
            
            span.add_tag("test", "value")
            span.log("test message")
        
        # Span should be finished
        assert span.end_time is not None
        assert TracingContext.get_current_span() is None
        
        # Should be recorded in global tracer
        traces = global_tracer.get_traces()
        assert len(traces) == 1
        assert traces[0].operation_name == "test-operation"

class TestIntegration:
    
    def test_end_to_end_flow(self):
        """Test complete header flow from producer to consumer"""
        global_tracer.clear()
        
        # Simulate producer creating headers
        trace_context = HeaderManager.create_trace_context("producer")
        feature_flags = FeatureFlags(
            enabled_features={"new_algo": True},
            user_segment="beta",
            experiment_id="exp-001"
        )
        
        # Combine headers
        headers = {}
        headers.update(trace_context.to_headers())
        headers.update(feature_flags.to_headers())
        headers[HeaderKeys.USER_ID] = "user-123"
        
        # Simulate consumer extracting headers
        extracted_trace = HeaderManager.extract_trace_context(headers)
        extracted_flags = HeaderManager.extract_feature_flags(headers)
        
        assert extracted_trace.trace_id == trace_context.trace_id
        assert extracted_flags.enabled_features["new_algo"] is True
        assert extracted_flags.user_segment == "beta"
        assert extracted_flags.experiment_id == "exp-001"
        assert headers[HeaderKeys.USER_ID] == "user-123"

if __name__ == "__main__":
    pytest.main([__file__])
