#!/bin/bash

# StreamSocial Timeline Ordering Demo Script
set -e

echo "🎬 StreamSocial Timeline Ordering Demo"
echo "===================================="

# Activate virtual environment
source venv/bin/activate
export PYTHONPATH="${PWD}:${PYTHONPATH}"

echo "🎯 Running automated demo scenarios..."

# Demo script to show functionality
python -c "
import time
import requests
import json
from datetime import datetime

# Demo configuration
DASHBOARD_URL = 'http://localhost:8000'
DEMO_USERS = ['alice', 'bob', 'charlie', 'diana']
POSTS_PER_USER = 3

def send_demo_message(user_id, content):
    try:
        response = requests.post(f'{DASHBOARD_URL}/api/send-message', 
                               json={'user_id': user_id, 'content': content},
                               timeout=5)
        return response.status_code == 200
    except:
        return False

def get_user_timeline(user_id):
    try:
        response = requests.get(f'{DASHBOARD_URL}/api/user-timeline/{user_id}', timeout=5)
        return response.json() if response.status_code == 200 else None
    except:
        return None

def check_dashboard_available():
    try:
        response = requests.get(DASHBOARD_URL, timeout=5)
        return response.status_code == 200
    except:
        return False

print('📡 Checking if dashboard is available...')
if not check_dashboard_available():
    print('❌ Dashboard not available at http://localhost:8000')
    print('   Please start the system with: ./start.sh')
    exit(1)

print('✅ Dashboard is available!')
print('')

print('📝 Sending demo messages...')
message_templates = [
    'Just posted a new photo! 📸',
    'Having a great day! ☀️',
    'Working on something exciting! 💻',
    'Coffee break time! ☕',
    'Weekend vibes! 🎉'
]

for user in DEMO_USERS:
    print(f'  📤 Sending messages for user: {user}')
    for i in range(POSTS_PER_USER):
        content = f'{message_templates[i % len(message_templates)]} (Message {i+1})'
        if send_demo_message(user, content):
            print(f'    ✅ Sent: {content[:30]}...')
        else:
            print(f'    ❌ Failed to send message')
        time.sleep(0.5)  # Small delay between messages
    print('')

print('⏳ Waiting for message processing...')
time.sleep(3)

print('📊 Checking user timelines...')
for user in DEMO_USERS:
    timeline = get_user_timeline(user)
    if timeline and timeline.get('timeline'):
        print(f'  👤 {user}: {timeline[\"message_count\"]} messages in timeline')
        # Check ordering
        messages = timeline['timeline']
        if len(messages) > 1:
            ordered = all(messages[i]['timestamp'] <= messages[i+1]['timestamp'] for i in range(len(messages)-1))
            print(f'    {'✅' if ordered else '❌'} Timeline ordering: {'Correct' if ordered else 'VIOLATION DETECTED'}')
        print('')

print('🎉 Demo completed!')
print('')
print('🌐 Visit http://localhost:8000 to interact with the dashboard')
print('🔍 Visit http://localhost:8080 to explore Kafka topics and partitions')
print('')
print('Try these interactive features:')
print('- Send custom messages')
print('- View user timelines')  
print('- Analyze partition distribution')
print('- Verify message ordering')
"

echo ""
echo "✅ Demo script completed!"
echo "🌐 Continue exploring at: http://localhost:8000"
