"""
Test Suite for Table-Table Joins
"""

import pytest
import json
import time
from datetime import datetime
from src.table_join_processor import (
    TableJoinProcessor, UserPreference, ContentMetadata, StateStore
)


def test_state_store():
    """Test state store operations"""
    store = StateStore("test-store")
    
    # Test put and get
    store.put("key1", "value1")
    assert store.get("key1") == "value1"
    
    # Test size
    assert store.size() == 1
    
    # Test delete
    store.delete("key1")
    assert store.get("key1") is None
    assert store.size() == 0


def test_match_scoring():
    """Test recommendation match scoring"""
    processor = TableJoinProcessor("localhost:9092", "test-group")
    
    preference = UserPreference(
        user_id="user_001",
        languages=["english", "spanish"],
        categories=["comedy", "drama"],
        topics=["entertainment", "travel"],
        weights={"language": 1.0, "category": 1.0, "topics": 1.0},
        updated_at=datetime.now().isoformat()
    )
    
    # Perfect match
    content_perfect = ContentMetadata(
        content_id="content_001",
        language="english",
        category="comedy",
        tags=["entertainment", "travel"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    score, matched = processor.compute_match_score(preference, content_perfect)
    assert score >= 0.8, f"Expected high score for perfect match, got {score}"
    assert "language" in matched
    assert "category" in matched
    
    # Partial match
    content_partial = ContentMetadata(
        content_id="content_002",
        language="french",
        category="comedy",
        tags=["sports"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    score_partial, _ = processor.compute_match_score(preference, content_partial)
    assert 0.3 <= score_partial < 0.6, f"Expected medium score for partial match, got {score_partial}"
    
    # No match
    content_none = ContentMetadata(
        content_id="content_003",
        language="japanese",
        category="sports",
        tags=["gaming"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    score_none, _ = processor.compute_match_score(preference, content_none)
    assert score_none < 0.5, f"Expected low score for no match, got {score_none}"


def test_preference_storage():
    """Test user preference storage and retrieval"""
    processor = TableJoinProcessor("localhost:9092", "test-group")
    
    preference = UserPreference(
        user_id="user_test",
        languages=["english"],
        categories=["tech"],
        topics=["ai"],
        weights={},
        updated_at=datetime.now().isoformat()
    )
    
    processor.user_prefs_store.put(preference.user_id, preference)
    
    retrieved = processor.user_prefs_store.get("user_test")
    assert retrieved is not None
    assert retrieved.user_id == "user_test"
    assert "english" in retrieved.languages


def test_content_storage():
    """Test content metadata storage"""
    processor = TableJoinProcessor("localhost:9092", "test-group")
    
    content = ContentMetadata(
        content_id="content_test",
        language="spanish",
        category="news",
        tags=["politics"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    processor.content_store.put(content.content_id, content)
    
    retrieved = processor.content_store.get("content_test")
    assert retrieved is not None
    assert retrieved.content_id == "content_test"
    assert retrieved.language == "spanish"


def test_join_operation():
    """Test join operation generates recommendations"""
    processor = TableJoinProcessor("localhost:9092", "test-group")
    
    # Add test data
    preference = UserPreference(
        user_id="user_join_test",
        languages=["english"],
        categories=["comedy"],
        topics=["entertainment"],
        weights={"language": 1.0, "category": 1.0, "topics": 1.0},
        updated_at=datetime.now().isoformat()
    )
    
    content = ContentMetadata(
        content_id="content_join_test",
        language="english",
        category="comedy",
        tags=["entertainment"],
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat()
    )
    
    processor.user_prefs_store.put(preference.user_id, preference)
    processor.content_store.put(content.content_id, content)
    
    # Execute join
    recommendations = processor.process_join(user_id=preference.user_id)
    
    assert len(recommendations) > 0, "Join should generate recommendations"
    assert recommendations[0].user_id == "user_join_test"
    assert recommendations[0].content_id == "content_join_test"
    assert recommendations[0].match_score >= 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
