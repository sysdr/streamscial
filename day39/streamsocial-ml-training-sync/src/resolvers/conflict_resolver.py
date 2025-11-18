from datetime import datetime
from typing import Dict, Any, Optional
import json
import structlog

logger = structlog.get_logger()

class ConflictResolver:
    """Handles conflict resolution for concurrent record updates"""
    
    @staticmethod
    def last_write_wins(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        """Simple LWW strategy - latest timestamp wins"""
        existing_ts = existing.get('updated_at') or existing.get('created_at')
        incoming_ts = incoming.get('updated_at') or incoming.get('created_at')
        
        if isinstance(existing_ts, str):
            existing_ts = datetime.fromisoformat(existing_ts.replace('Z', '+00:00'))
        if isinstance(incoming_ts, str):
            incoming_ts = datetime.fromisoformat(incoming_ts.replace('Z', '+00:00'))
        
        if incoming_ts and existing_ts and incoming_ts > existing_ts:
            logger.info("lww_resolution", winner="incoming", 
                       existing_ts=str(existing_ts), incoming_ts=str(incoming_ts))
            return incoming
        return existing
    
    @staticmethod
    def merge_user_profile(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        """Merge strategy for user profiles - preserves maximum values and unions"""
        merged = existing.copy()
        
        for key, value in incoming.items():
            if key in ('user_id', 'username'):
                merged[key] = value
            elif key == 'follower_count':
                merged[key] = max(existing.get(key, 0), value or 0)
            elif key == 'following_count':
                merged[key] = max(existing.get(key, 0), value or 0)
            elif key == 'interests':
                existing_interests = set(existing.get(key, []) or [])
                incoming_interests = set(value or [])
                merged[key] = list(existing_interests | incoming_interests)
            elif key == 'engagement_rate':
                merged[key] = max(existing.get(key, 0.0), value or 0.0)
            elif key == 'updated_at':
                existing_ts = existing.get(key)
                if isinstance(existing_ts, str):
                    existing_ts = datetime.fromisoformat(existing_ts.replace('Z', '+00:00'))
                if isinstance(value, str):
                    value = datetime.fromisoformat(value.replace('Z', '+00:00'))
                merged[key] = max(existing_ts, value) if existing_ts and value else value
            else:
                if value is not None:
                    merged[key] = value
        
        logger.info("merge_resolution", user_id=merged.get('user_id'))
        return merged
    
    @staticmethod
    def merge_content_metadata(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        """LWW for content metadata with engagement score preservation"""
        existing_ts = existing.get('updated_at')
        incoming_ts = incoming.get('updated_at')
        
        if isinstance(existing_ts, str):
            existing_ts = datetime.fromisoformat(existing_ts.replace('Z', '+00:00'))
        if isinstance(incoming_ts, str):
            incoming_ts = datetime.fromisoformat(incoming_ts.replace('Z', '+00:00'))
        
        if incoming_ts and existing_ts and incoming_ts > existing_ts:
            result = incoming.copy()
            # Preserve higher engagement score
            result['engagement_score'] = max(
                existing.get('engagement_score', 0.0),
                incoming.get('engagement_score', 0.0)
            )
            return result
        return existing
    
    @staticmethod
    def resolve(record_type: str, existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        """Route to appropriate resolution strategy"""
        if record_type == 'user_profile':
            return ConflictResolver.merge_user_profile(existing, incoming)
        elif record_type == 'content_metadata':
            return ConflictResolver.merge_content_metadata(existing, incoming)
        else:
            return ConflictResolver.last_write_wins(existing, incoming)


class VersionVector:
    """Version vector for strict ordering requirements"""
    
    def __init__(self, source: str, sequence: int, timestamp: datetime):
        self.source = source
        self.sequence = sequence
        self.timestamp = timestamp
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'source': self.source,
            'sequence': self.sequence,
            'timestamp': self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VersionVector':
        return cls(
            source=data['source'],
            sequence=data['sequence'],
            timestamp=datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        )
    
    def __gt__(self, other: 'VersionVector') -> bool:
        if self.source == other.source:
            return self.sequence > other.sequence
        return self.timestamp > other.timestamp
