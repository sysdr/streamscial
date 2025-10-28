#!/bin/bash
set -e

echo "🎬 StreamSocial Schema Evolution - Demo Script"
echo "=============================================="

# Activate virtual environment
source venv/bin/activate

echo "📤 Running producer demo..."
python -c "
from src.producer.profile_producer import StreamSocialProducer
producer = StreamSocialProducer()
print('🚀 Producing 30 sample profiles with mixed schema versions...')
producer.produce_profiles(30)
producer.close()
print('✅ Producer demo completed')
"

echo ""
echo "📥 Running consumer demo..."
python -c "
from src.consumer.profile_consumer import StreamSocialConsumer
consumer = StreamSocialConsumer(group_id='demo-consumer')
print('🎯 Consuming profiles for 20 seconds...')
profiles = consumer.consume_profiles(timeout_sec=20)
consumer.close()
print(f'✅ Consumer demo completed - processed {len(profiles)} profiles')

if profiles:
    print('\n📊 Sample processed profiles:')
    for i, profile in enumerate(profiles[:5]):
        print(f'  {i+1}. User {profile[\"user_id\"]} | Version: {profile[\"version\"]} | Premium: {profile[\"is_premium\"]}')
"

echo ""
echo "🎨 Starting dashboard demo..."
echo "   Dashboard will be available at: http://localhost:8050"
echo "   Press Ctrl+C to stop the dashboard"
echo ""

python src/dashboard/app.py
