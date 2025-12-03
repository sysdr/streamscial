import pytest
from datetime import datetime
from src.models.post import Post
from src.extractors.hashtag_extractor import HashtagExtractor

def test_basic_extraction():
    """Test basic hashtag extraction"""
    post = Post(
        post_id="1",
        user_id="user1",
        content="Loving #Python and #MachineLearning today!",
        created_at=datetime.now(),
        region="us-west"
    )
    
    extractor = HashtagExtractor()
    hashtags = extractor.extract(post)
    
    assert len(hashtags) == 2
    assert hashtags[0].tag == "python"
    assert hashtags[1].tag == "machinelearning"

def test_normalization():
    """Test hashtag normalization"""
    post = Post(
        post_id="1",
        user_id="user1",
        content="Check out #AI #Ai #ai",
        created_at=datetime.now()
    )
    
    extractor = HashtagExtractor()
    hashtags = extractor.extract(post)
    
    # All should be normalized to lowercase
    assert all(h.tag == "ai" for h in hashtags)

def test_no_hashtags():
    """Test post without hashtags"""
    post = Post(
        post_id="1",
        user_id="user1",
        content="Just a regular post without any tags",
        created_at=datetime.now()
    )
    
    extractor = HashtagExtractor()
    hashtags = extractor.extract(post)
    
    assert len(hashtags) == 0

def test_multiple_hashtags():
    """Test post with many hashtags"""
    post = Post(
        post_id="1",
        user_id="user1",
        content="#Tech #Innovation #AI #ML #DeepLearning #NLP #ComputerVision",
        created_at=datetime.now()
    )
    
    extractor = HashtagExtractor()
    hashtags = extractor.extract(post)
    
    assert len(hashtags) == 7
