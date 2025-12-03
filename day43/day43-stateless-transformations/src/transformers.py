"""Map and flatMap transformations for content enrichment."""

import re
import json
import time
import hashlib
from typing import Dict, List, Any
from config import MAX_MENTIONS_PER_POST, MAX_HASHTAGS_PER_POST

class ContentEnricher:
    """Enrich posts with additional metadata."""
    
    def __init__(self):
        # In production, load from Redis or database
        self.user_reputation_cache = {}
        self.category_keywords = {
            'technology': ['tech', 'ai', 'software', 'programming', 'code'],
            'sports': ['game', 'team', 'player', 'score', 'win'],
            'news': ['breaking', 'report', 'update', 'announced'],
            'entertainment': ['movie', 'music', 'show', 'concert', 'actor']
        }
    
    def enrich_post(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich post with reputation, category, sentiment.
        This is a map() operation: one post in, one enriched post out.
        """
        enriched = post.copy()
        
        # Add user reputation score
        user_id = post.get('user_id', 'unknown')
        enriched['user_reputation'] = self._get_user_reputation(user_id)
        
        # Classify category
        enriched['category'] = self._classify_category(post.get('content', ''))
        
        # Predict engagement
        enriched['engagement_prediction'] = self._predict_engagement(enriched)
        
        # Sentiment analysis
        enriched['sentiment'] = self._analyze_sentiment(post.get('content', ''))
        
        # Add processing metadata
        enriched['enriched_at'] = time.time()
        enriched['enrichment_version'] = '1.0'
        
        return enriched
    
    def _get_user_reputation(self, user_id: str) -> int:
        """Get user reputation score (0-100)."""
        if user_id in self.user_reputation_cache:
            return self.user_reputation_cache[user_id]
        
        # Simple hash-based reputation for demo
        hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        reputation = 50 + (hash_val % 50)  # Range: 50-100
        
        self.user_reputation_cache[user_id] = reputation
        return reputation
    
    def _classify_category(self, content: str) -> str:
        """Classify post into category."""
        content_lower = content.lower()
        
        for category, keywords in self.category_keywords.items():
            for keyword in keywords:
                if keyword in content_lower:
                    return category
        
        return 'general'
    
    def _predict_engagement(self, post: Dict[str, Any]) -> float:
        """Predict engagement probability."""
        # Simple heuristic (in production, use ML model)
        score = 0.5
        
        # High reputation users get higher engagement
        reputation = post.get('user_reputation', 50)
        score += (reputation - 50) / 100 * 0.3
        
        # Longer content tends to engage less
        content_len = len(post.get('content', ''))
        if content_len > 500:
            score -= 0.1
        elif content_len < 50:
            score -= 0.2
        
        # Posts with images engage more
        if post.get('has_image', False):
            score += 0.2
        
        return max(0.0, min(1.0, score))
    
    def _analyze_sentiment(self, content: str) -> str:
        """Simple sentiment analysis."""
        positive_words = ['love', 'great', 'awesome', 'amazing', 'happy']
        negative_words = ['hate', 'bad', 'terrible', 'awful', 'sad']
        
        content_lower = content.lower()
        pos_count = sum(1 for word in positive_words if word in content_lower)
        neg_count = sum(1 for word in negative_words if word in content_lower)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        return 'neutral'


class MentionExtractor:
    """Extract mentions and hashtags using flatMap."""
    
    def __init__(self):
        self.mention_pattern = re.compile(r'@(\w+)')
        self.hashtag_pattern = re.compile(r'#(\w+)')
    
    def extract_events(self, post: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract mention and hashtag events from post.
        This is a flatMap() operation: one post in, N events out.
        """
        events = []
        content = post.get('content', '')
        post_id = post.get('id', 'unknown')
        timestamp = time.time()
        
        # Extract mentions
        mentions = self.mention_pattern.findall(content)
        mentions = mentions[:MAX_MENTIONS_PER_POST]  # Limit explosion
        
        for mentioned_user in mentions:
            events.append({
                'type': 'mention_notification',
                'target_user': mentioned_user,
                'source_post_id': post_id,
                'source_user_id': post.get('user_id'),
                'timestamp': timestamp,
                'content_preview': content[:100]
            })
        
        # Extract hashtags
        hashtags = self.hashtag_pattern.findall(content)
        hashtags = hashtags[:MAX_HASHTAGS_PER_POST]  # Limit explosion
        
        for tag in hashtags:
            events.append({
                'type': 'hashtag_index',
                'tag': tag.lower(),
                'post_id': post_id,
                'user_id': post.get('user_id'),
                'timestamp': timestamp
            })
        
        return events


# Singleton instances
content_enricher = ContentEnricher()
mention_extractor = MentionExtractor()
