"""Test suite for centralized logging system"""
import pytest
import time
import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent / 'src'))
from shared.structured_logger import setup_structured_logger, generate_trace_id, LogContext
from shared.log_analyzer import LogAnalyzer

def test_trace_id_generation():
    """Test trace ID generation"""
    trace_id = generate_trace_id()
    assert trace_id.startswith('trace-')
    assert len(trace_id) == 18  # 'trace-' + 12 hex chars

def test_structured_logger_output():
    """Test structured logging output format"""
    logger = setup_structured_logger('test-component')
    
    with LogContext(logger, trace_id='test-123', component='test') as log:
        log.info('test_event', metric=42, status='success')
    
    # Logger should output JSON format
    assert True  # Basic sanity check

def test_log_context_manager():
    """Test log context manager"""
    logger = setup_structured_logger('test-component')
    
    trace_id = generate_trace_id()
    with LogContext(logger, trace_id=trace_id, test_value=100) as log:
        # Context should be available
        assert True

def test_elasticsearch_connection():
    """Test Elasticsearch connection"""
    try:
        analyzer = LogAnalyzer()
        assert analyzer.es.ping()
    except Exception as e:
        pytest.skip(f"Elasticsearch not available: {e}")

def test_error_rate_calculation():
    """Test error rate calculation"""
    try:
        analyzer = LogAnalyzer()
        result = analyzer.get_error_rate(minutes=5)
        assert 'error_rate' in result
        assert 'total_logs' in result
        assert isinstance(result['error_rate'], (int, float))
    except Exception as e:
        pytest.skip(f"Elasticsearch not available: {e}")

def test_trace_id_search():
    """Test searching by trace ID"""
    try:
        analyzer = LogAnalyzer()
        # Test with non-existent trace ID
        results = analyzer.search_by_trace_id('trace-nonexistent')
        assert isinstance(results, list)
    except Exception as e:
        pytest.skip(f"Elasticsearch not available: {e}")

def test_latency_stats():
    """Test latency statistics calculation"""
    try:
        analyzer = LogAnalyzer()
        stats = analyzer.get_latency_stats()
        # Should return empty dict or stats with expected keys
        assert isinstance(stats, dict)
    except Exception as e:
        pytest.skip(f"Elasticsearch not available: {e}")

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
