"""
Data models for hashtag analytics
"""
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime

@dataclass
class HashtagRecord:
    """Individual hashtag record from Kafka"""
    hashtag: str
    count: int
    window_start: datetime
    window_end: datetime
    partition: int
    offset: int
    processed_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        # Clean hashtag (remove # if present)
        if self.hashtag.startswith('#'):
            self.hashtag = self.hashtag[1:]
        self.hashtag = self.hashtag.lower()

@dataclass 
class HashtagBatch:
    """Batch of hashtag records for processing"""
    records: List[HashtagRecord] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    
    def add_record(self, record: HashtagRecord):
        """Add record to batch"""
        self.records.append(record)
        
    def clear(self):
        """Clear all records from batch"""
        self.records.clear()
        self.created_at = datetime.now()
        
    @property
    def size(self) -> int:
        """Get batch size"""
        return len(self.records)
        
    @property
    def unique_hashtags(self) -> int:
        """Get count of unique hashtags in batch"""
        return len(set(record.hashtag for record in self.records))

@dataclass
class TrendingHashtag:
    """Trending hashtag with analytics data"""
    hashtag: str
    count: int
    trend_score: float
    last_updated: datetime
    rank: Optional[int] = None
