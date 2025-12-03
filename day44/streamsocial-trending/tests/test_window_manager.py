import pytest
from datetime import datetime, timedelta
from src.models.post import Hashtag
from src.aggregators.window_manager import WindowManager

def test_window_creation():
    """Test window creation and management"""
    manager = WindowManager(
        window_size_minutes=30,
        advance_minutes=5,
        grace_minutes=5
    )
    
    timestamp = datetime(2025, 1, 1, 12, 0, 0)
    hashtag = Hashtag(
        tag="ai",
        post_id="1",
        user_id="user1",
        timestamp=timestamp,
        region="global",
        language="en",
        engagement_score=5.0
    )
    
    results = manager.add_to_window(hashtag)
    
    # Should create multiple overlapping windows
    assert len(results) > 0
    assert manager.window_count > 0

def test_overlapping_windows():
    """Test that hopping windows overlap correctly"""
    manager = WindowManager(
        window_size_minutes=30,
        advance_minutes=5,
        grace_minutes=5
    )
    
    timestamp = datetime(2025, 1, 1, 12, 15, 0)
    hashtag = Hashtag(
        tag="ml",
        post_id="1",
        user_id="user1",
        timestamp=timestamp,
        region="global",
        language="en",
        engagement_score=3.0
    )
    
    active_windows = manager.get_active_windows(timestamp)
    
    # With 30-minute windows advancing every 5 minutes,
    # this event should belong to 6 windows
    assert len(active_windows) == 6

def test_window_closure():
    """Test window closure after grace period"""
    manager = WindowManager(
        window_size_minutes=30,
        advance_minutes=5,
        grace_minutes=5
    )
    
    # Add hashtag at T=0
    hashtag = Hashtag(
        tag="test",
        post_id="1",
        user_id="user1",
        timestamp=datetime(2025, 1, 1, 12, 0, 0),
        region="global",
        language="en",
        engagement_score=1.0
    )
    manager.add_to_window(hashtag)
    
    # Check for closeable windows at T=40 (past grace period)
    current_time = datetime(2025, 1, 1, 12, 40, 0)
    closeable = manager.get_closeable_windows(current_time)
    
    # Some windows should be closeable
    assert len(closeable) > 0
