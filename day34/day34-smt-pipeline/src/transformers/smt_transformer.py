import json
import time
from datetime import datetime
from typing import Dict, Any, Optional

class SMTTransformer:
    """
    Simulates Kafka Connect SMT transformations in Python
    for demonstration purposes
    """
    
    def __init__(self, source_platform: str):
        self.source_platform = source_platform
        self.stats = {
            'total_processed': 0,
            'total_filtered': 0,
            'total_errors': 0
        }
    
    def extract_timestamp(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and normalize timestamp"""
        if self.source_platform == 'ios':
            # iOS: Unix seconds
            if 'ts' in message:
                message['timestamp'] = message.pop('ts') * 1000
        elif self.source_platform == 'android':
            # Android: ISO string
            if 'timestamp' in message:
                ts_str = message['timestamp']
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                message['timestamp'] = int(dt.timestamp() * 1000)
        elif self.source_platform == 'web':
            # Web: Unix milliseconds (already correct)
            if 'occurred_at' in message:
                message['timestamp'] = message.pop('occurred_at')
        
        return message
    
    def normalize_fields(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Rename fields to standard names"""
        field_mapping = {
            'ios': {
                'user': 'user_id',
                'postId': 'post_id',
                'action': 'action'
            },
            'android': {
                'userId': 'user_id',
                'post_id': 'post_id',
                'event_type': 'action'
            },
            'web': {
                'user_id': 'user_id',
                'target': 'post_id',
                'reaction_kind': 'action'
            }
        }
        
        mapping = field_mapping.get(self.source_platform, {})
        normalized = {}
        
        for old_key, new_key in mapping.items():
            if old_key in message:
                normalized[new_key] = message[old_key]
        
        # Preserve timestamp if present
        if 'timestamp' in message:
            normalized['timestamp'] = message['timestamp']
            
        return normalized
    
    def cast_types(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure correct data types"""
        if 'timestamp' in message:
            message['timestamp'] = int(message['timestamp'])
        if 'user_id' in message:
            message['user_id'] = str(message['user_id'])
        if 'post_id' in message:
            message['post_id'] = str(message['post_id'])
        if 'action' in message:
            message['action'] = str(message['action']).lower()
        
        return message
    
    def add_metadata(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Add source and processing metadata"""
        message['source'] = self.source_platform
        message['processed_at'] = int(time.time() * 1000)
        return message
    
    def filter_invalid(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter out invalid messages"""
        required_fields = ['user_id', 'post_id', 'action', 'timestamp']
        
        for field in required_fields:
            if field not in message or message[field] is None:
                self.stats['total_filtered'] += 1
                return None
        
        return message
    
    def transform(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply full transformation chain"""
        try:
            # Step 1: Extract and normalize timestamp
            message = self.extract_timestamp(message)
            
            # Step 2: Normalize field names
            message = self.normalize_fields(message)
            
            # Step 3: Cast to correct types
            message = self.cast_types(message)
            
            # Step 4: Add metadata
            message = self.add_metadata(message)
            
            # Step 5: Filter invalid
            message = self.filter_invalid(message)
            
            if message:
                self.stats['total_processed'] += 1
            
            return message
            
        except Exception as e:
            self.stats['total_errors'] += 1
            print(f"❌ Transform error: {e}")
            return None
    
    def get_stats(self) -> Dict[str, int]:
        """Return transformation statistics"""
        return self.stats.copy()

if __name__ == '__main__':
    # Test transformation
    ios_msg = {
        'action': 'LIKE',
        'user': 'alice123',
        'postId': 'p789',
        'ts': int(time.time())
    }
    
    transformer = SMTTransformer('ios')
    result = transformer.transform(ios_msg)
    print(f"Original: {ios_msg}")
    print(f"Transformed: {result}")
