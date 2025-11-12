"""
Mock social platform API clients
In production, these would call real APIs
"""
import time
import random
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta


class BaseSocialClient:
    """Base client for social platforms"""
    
    def __init__(self, api_base: str, access_token: str):
        self.api_base = api_base
        self.access_token = access_token
        self.request_count = 0
        
    def _generate_post(self, account_id: str, post_id: int, timestamp: datetime) -> Dict[str, Any]:
        """Generate realistic post data"""
        # Create deterministic content based on post_id
        seed = f"{account_id}_{post_id}"
        hash_val = int(hashlib.md5(seed.encode()).hexdigest(), 16)
        
        texts = [
            "Just launched our new feature! Check it out 🚀",
            "Excited to share our latest blog post on system design",
            "Great team collaboration today on the StreamSocial platform",
            "New insights on event-driven architectures",
            "Kafka integration patterns that scale to millions of events",
        ]
        
        return {
            'id': f"post_{post_id}",
            'account_id': account_id,
            'text': texts[hash_val % len(texts)],
            'created_at': timestamp.isoformat(),
            'likes': hash_val % 1000,
            'retweets': hash_val % 500,
            'platform': self.__class__.__name__.replace('Client', '').lower(),
        }


class TwitterClient(BaseSocialClient):
    """Twitter API Client"""
    
    def fetch_posts(self, account_id: str, since_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch posts since last ID"""
        self.request_count += 1
        
        # Simulate API latency
        time.sleep(0.05)
        
        # Generate new posts
        posts = []
        start_id = int(since_id.split('_')[1]) if since_id else 1
        
        # Generate 5-15 new posts
        num_posts = random.randint(5, 15)
        base_time = datetime.now() - timedelta(minutes=num_posts)
        
        for i in range(num_posts):
            post_id = start_id + i + 1
            timestamp = base_time + timedelta(minutes=i)
            posts.append(self._generate_post(account_id, post_id, timestamp))
            
        return posts


class LinkedInClient(BaseSocialClient):
    """LinkedIn API Client"""
    
    def fetch_posts(self, account_id: str, since_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch posts since last ID"""
        self.request_count += 1
        
        # Simulate API latency
        time.sleep(0.08)
        
        # Generate new posts (fewer than Twitter)
        posts = []
        start_id = int(since_id.split('_')[1]) if since_id else 1
        
        num_posts = random.randint(2, 8)
        base_time = datetime.now() - timedelta(minutes=num_posts * 5)
        
        for i in range(num_posts):
            post_id = start_id + i + 1
            timestamp = base_time + timedelta(minutes=i * 5)
            post = self._generate_post(account_id, post_id, timestamp)
            post['impressions'] = post['likes'] * 10  # LinkedIn-specific field
            posts.append(post)
            
        return posts


def create_client(platform: str, api_base: str, access_token: str) -> BaseSocialClient:
    """Factory method to create appropriate client"""
    clients = {
        'twitter': TwitterClient,
        'linkedin': LinkedInClient,
    }
    
    client_class = clients.get(platform.lower())
    if not client_class:
        raise ValueError(f"Unknown platform: {platform}")
        
    return client_class(api_base, access_token)
