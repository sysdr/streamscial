"""Spam and policy filters for content moderation."""

import re
import json
import time
from typing import Dict, List, Tuple, Any
from config import (
    SPAM_KEYWORDS, SPAM_ML_THRESHOLD, SPAM_EMOJI_THRESHOLD,
    PROHIBITED_KEYWORDS, MATURE_CONTENT_KEYWORDS, CLICKBAIT_PATTERNS
)

class SpamFilter:
    """Multi-tier spam detection filter."""
    
    def __init__(self):
        # Compile regex patterns for performance
        self.spam_patterns = [
            re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
            for kw in SPAM_KEYWORDS
        ]
        self.emoji_pattern = re.compile(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF]')
        
        # Simple ML model weights (in production, load trained model)
        self.ml_weights = {
            'url_count': -0.3,
            'all_caps_ratio': -0.4,
            'exclamation_count': -0.2,
            'emoji_count': -0.1,
            'content_length': 0.05
        }
    
    def is_spam(self, post: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check if post is spam using three-tier approach.
        Returns (is_spam: bool, reason: str)
        """
        content = post.get('content', '')
        
        # Tier 1: Fast keyword matching (99.9% throughput)
        for pattern in self.spam_patterns:
            if pattern.search(content):
                return True, 'keyword_match'
        
        # Check emoji spam
        emoji_count = len(self.emoji_pattern.findall(content))
        if emoji_count > SPAM_EMOJI_THRESHOLD:
            return True, 'emoji_spam'
        
        # Tier 2: ML scoring (95% throughput)
        spam_score = self._calculate_spam_score(post)
        if spam_score > SPAM_ML_THRESHOLD:
            return True, 'ml_detected'
        
        # Tier 3: Behavior analysis
        if self._check_burst_pattern(post):
            return True, 'behavior_anomaly'
        
        return False, 'clean'
    
    def _calculate_spam_score(self, post: Dict[str, Any]) -> float:
        """Calculate spam probability using simple feature-based model."""
        content = post.get('content', '')
        
        # Extract features
        content_len = len(content)
        features = {
            'url_count': content.count('http'),
            'all_caps_ratio': sum(1 for c in content if c.isupper()) / max(content_len, 1),
            'exclamation_count': content.count('!'),
            'emoji_count': len(self.emoji_pattern.findall(content)),
            'content_length': content_len / 1000.0  # Normalize to 0-1 range (assuming max 1000 chars)
        }
        
        # Simple weighted sum (in production, use trained model)
        score = 0.5  # Base score
        for feature, value in features.items():
            score += self.ml_weights.get(feature, 0) * value
        
        # Normalize to [0, 1]
        return max(0.0, min(1.0, score))
    
    def _check_burst_pattern(self, post: Dict[str, Any]) -> bool:
        """Check for burst posting patterns (simplified)."""
        # In production, check Redis for recent post timestamps
        # For now, simple heuristic based on user reputation
        user_reputation = post.get('user_reputation', 50)
        return user_reputation < 20  # Flag low-reputation users


class PolicyFilter:
    """Content policy enforcement filter."""
    
    def __init__(self):
        self.prohibited_patterns = [
            re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
            for kw in PROHIBITED_KEYWORDS
        ]
        self.mature_patterns = [
            re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
            for kw in MATURE_CONTENT_KEYWORDS
        ]
        self.clickbait_patterns = [
            re.compile(pattern, re.IGNORECASE)
            for pattern in CLICKBAIT_PATTERNS
        ]
    
    def check_policy(self, post: Dict[str, Any]) -> Tuple[str, List[str]]:
        """
        Check content policy violations.
        Returns (action: str, violations: List[str])
        Actions: 'ALLOW', 'REMOVE', 'RESTRICT', 'DEPRIORITIZE'
        """
        content = post.get('content', '')
        violations = []
        
        # Check prohibited content
        for pattern in self.prohibited_patterns:
            if pattern.search(content):
                violations.append('prohibited_content')
                return 'REMOVE', violations
        
        # Check mature content
        for pattern in self.mature_patterns:
            if pattern.search(content):
                violations.append('mature_content')
        
        if violations:
            return 'RESTRICT', violations
        
        # Check for clickbait/low quality
        for pattern in self.clickbait_patterns:
            if pattern.search(content):
                violations.append('low_quality')
                return 'DEPRIORITIZE', violations
        
        return 'ALLOW', []
    
    def passes_policy(self, post: Dict[str, Any]) -> bool:
        """Quick check if post passes policy (for filter operation)."""
        action, _ = self.check_policy(post)
        return action in ['ALLOW', 'DEPRIORITIZE']


# Singleton instances
spam_filter = SpamFilter()
policy_filter = PolicyFilter()
