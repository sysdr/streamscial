#!/bin/bash
set -e

echo "=============================================="
echo "StreamSocial ML Training Data Sync Demo"
echo "=============================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source venv/bin/activate

# Check services
echo "Checking service health..."

echo ""
echo "1. Kafka Connect Status:"
curl -s http://localhost:8083/ | python -m json.tool || echo "Connect not available"

echo ""
echo "2. Testing Conflict Resolution:"
python -c "
from src.resolvers.conflict_resolver import ConflictResolver
from datetime import datetime, timedelta

resolver = ConflictResolver()
t1 = datetime.utcnow()
t2 = t1 + timedelta(seconds=10)

# Test merge strategy
existing = {
    'user_id': 1,
    'follower_count': 1000,
    'interests': ['music', 'sports'],
    'engagement_rate': 0.12,
    'updated_at': t1
}
incoming = {
    'user_id': 1,
    'follower_count': 950,
    'interests': ['sports', 'tech'],
    'engagement_rate': 0.10,
    'updated_at': t2
}

result = resolver.merge_user_profile(existing, incoming)
print('Merge Result:')
print(f'  Follower count: {result[\"follower_count\"]} (kept higher: 1000)')
print(f'  Interests: {result[\"interests\"]} (union)')
print(f'  Engagement: {result[\"engagement_rate\"]} (kept higher: 0.12)')
"

echo ""
echo "3. Testing Feature Extraction:"
python -c "
from src.processors.feature_processor import FeatureProcessor

processor = FeatureProcessor('localhost:9092')

profile = {
    'user_id': 42,
    'follower_count': 15000,
    'following_count': 500,
    'account_age_days': 730,
    'engagement_rate': 0.085,
    'interests': ['tech', 'ai', 'music']
}

features = processor.extract_user_features(profile)
print('User Feature Extraction:')
for key, value in features.items():
    if key != 'feature_timestamp':
        print(f'  {key}: {value}')
"

echo ""
echo "4. Database Records:"
python -c "
from src.models import get_session, UserProfile, UserInteraction, ContentMetadata

session = get_session()
users = session.query(UserProfile).count()
interactions = session.query(UserInteraction).count()
content = session.query(ContentMetadata).count()
session.close()

print(f'  User Profiles: {users}')
print(f'  Content Metadata: {content}')
print(f'  User Interactions: {interactions}')
"

echo ""
echo "5. API Endpoints:"
echo "  - Dashboard: http://localhost:8080"
echo "  - Health: http://localhost:8080/api/health"
echo "  - Metrics: http://localhost:8080/api/metrics"
echo "  - Connectors: http://localhost:8080/api/connectors"

echo ""
echo "Demo complete!"
