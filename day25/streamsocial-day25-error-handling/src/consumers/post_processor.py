import json
import re
from typing import Dict, Any
from datetime import datetime

class StreamSocialPostProcessor:
    def __init__(self):
        self.processed_posts = []
        
    def process_social_post(self, message: Dict[str, Any], context: Dict):
        """Process StreamSocial post with validation"""
        
        # Validate required fields
        required_fields = ['user_id', 'content', 'timestamp']
        for field in required_fields:
            if field not in message:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate data types
        if not isinstance(message['user_id'], (str, int)):
            raise ValueError("Invalid user_id type")
            
        if not isinstance(message['content'], str):
            raise ValueError("Invalid content type")
        
        # Content validation
        if len(message['content']) > 280:
            raise ValueError("Content exceeds maximum length")
            
        # Clean and process content
        cleaned_content = self._clean_content(message['content'])
        
        # Extract metadata
        hashtags = self._extract_hashtags(cleaned_content)
        mentions = self._extract_mentions(cleaned_content)
        
        processed_post = {
            'id': f"post_{context['offset']}_{message['user_id']}",
            'user_id': str(message['user_id']),
            'content': cleaned_content,
            'hashtags': hashtags,
            'mentions': mentions,
            'timestamp': message['timestamp'],
            'processed_at': datetime.utcnow().isoformat(),
            'partition': context['partition'],
            'offset': context['offset']
        }
        
        # Optional fields (safe to skip if missing/malformed)
        try:
            if 'location' in message:
                processed_post['location'] = message['location']
            if 'media_urls' in message:
                processed_post['media_urls'] = message['media_urls']
            if 'tags' in message:
                processed_post['tags'] = message['tags']
        except Exception:
            # Skip optional field processing errors
            pass
            
        self.processed_posts.append(processed_post)
        print(f"✅ Processed post from user {message['user_id']}: {cleaned_content[:50]}...")
        
    def _clean_content(self, content: str) -> str:
        """Clean post content"""
        # Remove null bytes and control characters
        content = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', content)
        return content.strip()
        
    def _extract_hashtags(self, content: str) -> list:
        """Extract hashtags from content"""
        return re.findall(r'#(\w+)', content)
        
    def _extract_mentions(self, content: str) -> list:
        """Extract user mentions from content"""  
        return re.findall(r'@(\w+)', content)
        
    def get_processed_posts(self) -> list:
        """Get all processed posts"""
        return self.processed_posts.copy()
