import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from transformers.smt_transformer import SMTTransformer
import time

def test_ios_transformation():
    transformer = SMTTransformer('ios')
    
    message = {
        'action': 'LIKE',
        'user': 'alice123',
        'postId': 'p789',
        'ts': int(time.time())
    }
    
    result = transformer.transform(message)
    
    assert result is not None
    assert result['user_id'] == 'alice123'
    assert result['post_id'] == 'p789'
    assert result['action'] == 'like'
    assert result['source'] == 'ios'
    assert 'timestamp' in result
    assert 'processed_at' in result
    
    print("✅ iOS transformation test passed")

def test_android_transformation():
    transformer = SMTTransformer('android')
    
    message = {
        'event_type': 'comment',
        'userId': 'bob456',
        'post_id': 'p234',
        'timestamp': '2025-01-01T00:00:00Z'
    }
    
    result = transformer.transform(message)
    
    assert result is not None
    assert result['user_id'] == 'bob456'
    assert result['post_id'] == 'p234'
    assert result['action'] == 'comment'
    assert result['source'] == 'android'
    
    print("✅ Android transformation test passed")

def test_web_transformation():
    transformer = SMTTransformer('web')
    
    message = {
        'type': 'reaction',
        'user_id': 'carol789',
        'target': 'p567',
        'reaction_kind': 'share',
        'occurred_at': int(time.time() * 1000)
    }
    
    result = transformer.transform(message)
    
    assert result is not None
    assert result['user_id'] == 'carol789'
    assert result['post_id'] == 'p567'
    assert result['action'] == 'share'
    assert result['source'] == 'web'
    
    print("✅ Web transformation test passed")

def test_invalid_message_filtering():
    transformer = SMTTransformer('ios')
    
    # Missing required field
    message = {
        'action': 'LIKE',
        'postId': 'p789',
        'ts': int(time.time())
    }
    
    result = transformer.transform(message)
    
    assert result is None
    assert transformer.stats['total_filtered'] > 0
    
    print("✅ Invalid message filtering test passed")

if __name__ == '__main__':
    test_ios_transformation()
    test_android_transformation()
    test_web_transformation()
    test_invalid_message_filtering()
    print("\n🎉 All tests passed!")
