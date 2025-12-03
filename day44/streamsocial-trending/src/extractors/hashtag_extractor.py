import re
from typing import List
from datetime import datetime
from src.models.post import Post, Hashtag

class HashtagExtractor:
    """Extracts hashtags from post content"""
    
    HASHTAG_PATTERN = re.compile(r'#(\w+)')
    
    def __init__(self):
        self.extraction_count = 0
    
    def extract(self, post: Post) -> List[Hashtag]:
        """
        Extract all hashtags from post content.
        Returns a list of Hashtag objects.
        """
        hashtags = []
        matches = self.HASHTAG_PATTERN.findall(post.content)
        
        for tag in matches:
            # Normalize: lowercase, remove special chars
            normalized_tag = tag.lower().strip()
            if len(normalized_tag) >= 2:  # Minimum length
                hashtag = Hashtag(
                    tag=normalized_tag,
                    post_id=post.post_id,
                    user_id=post.user_id,
                    timestamp=post.created_at,
                    region=post.region,
                    language=post.language,
                    engagement_score=post.engagement_score
                )
                hashtags.append(hashtag)
                self.extraction_count += 1
        
        return hashtags
    
    def get_metrics(self) -> dict:
        return {'total_extractions': self.extraction_count}
