"""Tests for content enrichment transformers."""

import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from transformers import content_enricher, mention_extractor

def test_content_enrichment():
    """Test post enrichment adds required fields."""
    post = {
        'id': 'post123',
        'user_id': 'user456',
        'content': 'Check out this amazing tech article!',
        'has_image': True
    }
    
    enriched = content_enricher.enrich_post(post)
    
    assert 'user_reputation' in enriched
    assert 'category' in enriched
    assert 'engagement_prediction' in enriched
    assert 'sentiment' in enriched
    assert enriched['category'] == 'technology'
    assert 0 <= enriched['engagement_prediction'] <= 1

def test_sentiment_detection():
    """Test sentiment analysis."""
    positive_post = {
        'content': 'I love this amazing product! So happy!',
        'user_id': 'user123'
    }
    enriched = content_enricher.enrich_post(positive_post)
    assert enriched['sentiment'] == 'positive'
    
    negative_post = {
        'content': 'This is terrible and awful, I hate it',
        'user_id': 'user123'
    }
    enriched = content_enricher.enrich_post(negative_post)
    assert enriched['sentiment'] == 'negative'

def test_mention_extraction():
    """Test mention extraction creates events."""
    post = {
        'id': 'post123',
        'user_id': 'user456',
        'content': 'Hey @alice and @bob, check this out!'
    }
    
    events = mention_extractor.extract_events(post)
    
    # Should create 2 mention events
    mention_events = [e for e in events if e['type'] == 'mention_notification']
    assert len(mention_events) == 2
    
    targets = {e['target_user'] for e in mention_events}
    assert 'alice' in targets
    assert 'bob' in targets

def test_hashtag_extraction():
    """Test hashtag extraction creates index events."""
    post = {
        'id': 'post789',
        'user_id': 'user123',
        'content': 'Loving #python and #coding today! #tech'
    }
    
    events = mention_extractor.extract_events(post)
    
    # Should create 3 hashtag events
    hashtag_events = [e for e in events if e['type'] == 'hashtag_index']
    assert len(hashtag_events) == 3
    
    tags = {e['tag'] for e in hashtag_events}
    assert 'python' in tags
    assert 'coding' in tags
    assert 'tech' in tags

def test_flatmap_explosion_limit():
    """Test that flatMap limits output to prevent explosion."""
    # Create post with many mentions
    mentions = ' '.join([f'@user{i}' for i in range(20)])
    post = {
        'id': 'post999',
        'user_id': 'user000',
        'content': f'Mentioning everyone: {mentions}'
    }
    
    events = mention_extractor.extract_events(post)
    mention_events = [e for e in events if e['type'] == 'mention_notification']
    
    # Should be limited to MAX_MENTIONS_PER_POST (10)
    assert len(mention_events) <= 10
