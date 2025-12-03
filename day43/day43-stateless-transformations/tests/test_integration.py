"""Integration tests for the complete pipeline."""

import pytest
import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from stream_processor import StreamProcessor

def test_end_to_end_pipeline():
    """Test complete pipeline processing."""
    # Test record
    test_post = {
        'id': 'test123',
        'user_id': 'user456',
        'content': 'Excited to share my new tech project!',
        'has_image': True,
        'timestamp': 1234567890
    }
    
    processor = StreamProcessor()
    
    # Process through pipeline
    result = processor.process_record(test_post)
    
    # Verify transformations applied
    assert result is not None
    assert result['id'] == 'test123'
    assert 'user_reputation' in result
    assert 'category' in result
    assert 'engagement_prediction' in result
    assert 'policy_action' in result
    assert result['policy_action'] == 'ALLOW'

def test_spam_filtered_out():
    """Test that spam posts are filtered out."""
    spam_post = {
        'id': 'spam123',
        'user_id': 'spammer',
        'content': 'BUY NOW!!! Click here for free money!!!',
        'timestamp': 1234567890
    }
    
    processor = StreamProcessor()
    result = processor.process_record(spam_post)
    
    # Should return None (filtered out)
    assert result is None

def test_policy_violation_removed():
    """Test that policy violations are removed."""
    violation_post = {
        'id': 'bad123',
        'user_id': 'user789',
        'content': 'This post contains violence and explicit content',
        'timestamp': 1234567890
    }
    
    processor = StreamProcessor()
    result = processor.process_record(violation_post)
    
    # Should return None (removed)
    assert result is None
