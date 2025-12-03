"""Configuration for StreamSocial content moderation pipeline."""

import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_GROUP_ID = 'streamsocial-moderation-pipeline'

# Topics
TOPIC_RAW_POSTS = 'social.posts.raw'
TOPIC_SPAM_CHECKED = 'social.posts.spam-checked'
TOPIC_POLICY_CHECKED = 'social.posts.policy-checked'
TOPIC_MODERATED = 'social.posts.moderated'
TOPIC_NOTIFICATIONS = 'social.notifications'

# Redis Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

# Spam Filter Configuration
SPAM_KEYWORDS = [
    'buy now', 'click here', 'limited time', 'act fast',
    'free money', 'get rich', 'guaranteed', '100% free',
    'no cost', 'risk free', 'call now', 'order now'
]

SPAM_ML_THRESHOLD = 0.85
SPAM_EMOJI_THRESHOLD = 10  # Max emojis before flagging

# Policy Configuration
PROHIBITED_KEYWORDS = [
    'violence', 'illegal', 'explicit'
]

MATURE_CONTENT_KEYWORDS = [
    'alcohol', 'gambling', 'mature'
]

CLICKBAIT_PATTERNS = [
    r'you won\'t believe',
    r'shocking',
    r'doctors hate',
    r'this one trick'
]

# User Reputation Thresholds
MIN_REPUTATION_FOR_SKIP = 90  # Skip moderation for high-rep users
NEW_USER_DAYS = 30

# Processing Configuration
MAX_MENTIONS_PER_POST = 10
MAX_HASHTAGS_PER_POST = 10
BATCH_SIZE = 100
PROCESSING_TIMEOUT_MS = 30000

# Monitoring
METRICS_PORT = 8000
DASHBOARD_PORT = 5050
