"""
Unit tests for hashtag models
"""
import pytest
from datetime import datetime
from src.streamsocial.models.hashtag_model import HashtagRecord, HashtagBatch

class TestHashtagRecord:
    def test_hashtag_record_creation(self):
        record = HashtagRecord(
            hashtag="python",
            count=100,
            window_start=datetime.now(),
            window_end=datetime.now(),
            partition=0,
            offset=123
        )
        
        assert record.hashtag == "python"
        assert record.count == 100
        
    def test_hashtag_cleaning(self):
        record = HashtagRecord(
            hashtag="#Python",
            count=100,
            window_start=datetime.now(),
            window_end=datetime.now(),
            partition=0,
            offset=123
        )
        
        assert record.hashtag == "python"  # Should be lowercase without #

class TestHashtagBatch:
    def test_batch_creation(self):
        batch = HashtagBatch()
        assert batch.size == 0
        assert batch.unique_hashtags == 0
        
    def test_add_record(self):
        batch = HashtagBatch()
        record = HashtagRecord(
            hashtag="test",
            count=1,
            window_start=datetime.now(),
            window_end=datetime.now(),
            partition=0,
            offset=1
        )
        
        batch.add_record(record)
        assert batch.size == 1
        assert batch.unique_hashtags == 1
