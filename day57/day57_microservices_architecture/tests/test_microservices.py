"""Integration tests for microservices"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import requests
import time
import uuid

BASE_URLS = {
    'user': 'http://localhost:8001',
    'content': 'http://localhost:8002',
    'notification': 'http://localhost:8003'
}

def test_services_healthy():
    """Test all services are healthy"""
    for name, url in BASE_URLS.items():
        response = requests.get(f"{url}/health")
        assert response.status_code == 200
        assert response.json()['status'] == 'healthy'
        print(f"✓ {name.capitalize()} service is healthy")

def test_user_registration_flow():
    """Test user registration and event propagation"""
    # Register user
    email = f"test_{uuid.uuid4()}@example.com"
    username = f"user_{uuid.uuid4().hex[:8]}"
    
    response = requests.post(
        f"{BASE_URLS['user']}/users",
        json={"email": email, "username": username}
    )
    assert response.status_code == 200
    data = response.json()
    assert data['status'] == 'success'
    user_id = data['user']['user_id']
    print(f"✓ User registered: {user_id}")
    
    # Wait for event propagation
    time.sleep(3)
    
    # Verify content service cached user
    metrics = requests.get(f"{BASE_URLS['content']}/metrics").json()
    assert metrics['cached_users'] > 0
    print(f"✓ Content service cached user (total: {metrics['cached_users']})")
    
    # Verify notification was sent
    notifications = requests.get(f"{BASE_URLS['notification']}/notifications/{user_id}").json()
    assert len(notifications['notifications']) > 0
    assert notifications['notifications'][0]['event_type'] == 'UserRegistered'
    print(f"✓ Welcome notification sent")
    
    return user_id

def test_post_creation_saga():
    """Test post creation saga with notifications"""
    # First register a user
    user_id = test_user_registration_flow()
    
    # Create post
    content = f"Test post {uuid.uuid4()}"
    response = requests.post(
        f"{BASE_URLS['content']}/posts",
        json={"user_id": user_id, "content": content}
    )
    assert response.status_code == 200
    post_id = response.json()['post']['post_id']
    print(f"✓ Post created: {post_id}")
    
    # Wait for event propagation
    time.sleep(3)
    
    # Verify post exists
    posts = requests.get(f"{BASE_URLS['content']}/posts").json()
    assert len(posts['posts']) > 0
    print(f"✓ Post retrieved (total posts: {len(posts['posts'])})")
    
    # Verify notification sent
    notifications = requests.get(f"{BASE_URLS['notification']}/notifications/{user_id}").json()
    post_notifications = [n for n in notifications['notifications'] if n['event_type'] == 'PostCreated']
    assert len(post_notifications) > 0
    print(f"✓ Post creation notification sent")
    
    return post_id, user_id

def test_like_post_saga():
    """Test like post saga"""
    post_id, user_id = test_post_creation_saga()
    
    # Like post
    response = requests.post(
        f"{BASE_URLS['content']}/posts/{post_id}/like",
        json={"user_id": user_id}
    )
    assert response.status_code == 200
    print(f"✓ Post liked")
    
    # Wait for event propagation
    time.sleep(3)
    
    # Verify notification sent
    notifications = requests.get(f"{BASE_URLS['notification']}/notifications/{user_id}").json()
    like_notifications = [n for n in notifications['notifications'] if n['event_type'] == 'PostLiked']
    assert len(like_notifications) > 0
    print(f"✓ Like notification sent")

def test_eventual_consistency():
    """Test eventual consistency behavior"""
    # Register user
    response = requests.post(
        f"{BASE_URLS['user']}/users",
        json={"email": f"test_{uuid.uuid4()}@example.com", "username": f"user_{uuid.uuid4().hex[:8]}"}
    )
    user_id = response.json()['user']['user_id']
    
    # Immediately try to create post (before event propagates)
    response = requests.post(
        f"{BASE_URLS['content']}/posts",
        json={"user_id": user_id, "content": "Immediate post"}
    )
    # Should succeed even if user not cached yet
    assert response.status_code == 200
    print(f"✓ Eventual consistency: post created before user cached")
    
    # Wait and verify cache eventually consistent
    time.sleep(3)
    metrics = requests.get(f"{BASE_URLS['content']}/metrics").json()
    assert metrics['cached_users'] > 0
    print(f"✓ Eventually consistent: user now cached")

if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
