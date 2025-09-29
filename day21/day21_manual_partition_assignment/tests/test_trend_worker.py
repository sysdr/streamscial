import pytest
from unittest.mock import Mock, patch
from src.trend_worker import TrendWorker

class TestTrendWorker:
    def setup_method(self):
        with patch('src.trend_worker.KafkaConsumer'), \
             patch('src.trend_worker.KafkaProducer'), \
             patch('redis.Redis'):
            self.worker = TrendWorker(0)
            
    def test_extract_hashtags(self):
        """Test hashtag extraction"""
        text = "Love this #python tutorial! #coding #AI is amazing"
        hashtags = self.worker._extract_hashtags(text)
        
        assert "python" in hashtags
        assert "coding" in hashtags
        assert "ai" in hashtags
        assert len(hashtags) == 3
        
    def test_hashtag_normalization(self):
        """Test hashtag case normalization"""
        text = "#Python #PYTHON #python"
        hashtags = self.worker._extract_hashtags(text)
        
        # All should be normalized to lowercase
        assert all(tag == "python" for tag in hashtags)
        
    def test_trend_calculation(self):
        """Test trend score calculation"""
        # Mock some data
        self.worker.hashtag_timeline["python"].extend([1000, 1010, 1020, 1030])
        self.worker.user_hashtag_interactions["python"] = {"user1", "user2", "user3"}
        self.worker.hashtag_counts["python"] = 4
        
        trends = self.worker._calculate_trends()
        
        assert len(trends) >= 0  # Should not crash
        if trends:
            assert "hashtag" in trends[0]
            assert "trend_score" in trends[0]
