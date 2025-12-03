"""Tests for spam and policy filters."""

import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from filters import spam_filter, policy_filter

def test_spam_keyword_detection():
    """Test spam detection with keywords."""
    spam_post = {
        'content': 'BUY NOW!!! Limited time offer!!!',
        'user_id': 'user123',
        'user_reputation': 50
    }
    is_spam, reason = spam_filter.is_spam(spam_post)
    assert is_spam == True
    assert reason == 'keyword_match'

def test_clean_post_passes():
    """Test clean post passes spam filter."""
    clean_post = {
        'content': 'Just sharing my thoughts on the latest tech trends',
        'user_id': 'user456',
        'user_reputation': 75
    }
    is_spam, reason = spam_filter.is_spam(clean_post)
    assert is_spam == False
    assert reason == 'clean'

def test_emoji_spam_detection():
    """Test excessive emoji spam detection."""
    emoji_spam = {
        'content': '🔥' * 15 + ' Check this out!',
        'user_id': 'user789',
        'user_reputation': 50
    }
    is_spam, reason = spam_filter.is_spam(emoji_spam)
    assert is_spam == True
    assert reason == 'emoji_spam'

def test_policy_prohibited_content():
    """Test prohibited content detection."""
    prohibited_post = {
        'content': 'This contains violence and explicit content',
        'user_id': 'user123'
    }
    action, violations = policy_filter.check_policy(prohibited_post)
    assert action == 'REMOVE'
    assert 'prohibited_content' in violations

def test_policy_mature_content():
    """Test mature content detection."""
    mature_post = {
        'content': 'Discussion about alcohol and gambling',
        'user_id': 'user456'
    }
    action, violations = policy_filter.check_policy(mature_post)
    assert action == 'RESTRICT'
    assert 'mature_content' in violations

def test_policy_clickbait():
    """Test clickbait detection."""
    clickbait_post = {
        'content': 'You won\'t believe what happened next!',
        'user_id': 'user789'
    }
    action, violations = policy_filter.check_policy(clickbait_post)
    assert action == 'DEPRIORITIZE'
    assert 'low_quality' in violations

def test_policy_clean_post():
    """Test clean post passes policy check."""
    clean_post = {
        'content': 'Excited to share my new project!',
        'user_id': 'user123'
    }
    action, violations = policy_filter.check_policy(clean_post)
    assert action == 'ALLOW'
    assert len(violations) == 0
