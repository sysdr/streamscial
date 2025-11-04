-- Initialize StreamSocial database schema
CREATE TABLE IF NOT EXISTS trending_hashtags (
    hashtag VARCHAR(255) PRIMARY KEY,
    count BIGINT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    trend_score DECIMAL(10,4) DEFAULT 0,
    window_start TIMESTAMP,
    window_end TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trending_hashtags_count 
ON trending_hashtags(count DESC);

CREATE INDEX IF NOT EXISTS idx_trending_hashtags_updated 
ON trending_hashtags(last_updated DESC);

-- Insert some sample data
INSERT INTO trending_hashtags (hashtag, count, trend_score, window_start, window_end) 
VALUES 
    ('python', 1250, 1250.0, NOW() - INTERVAL '5 minutes', NOW()),
    ('kafka', 980, 980.0, NOW() - INTERVAL '5 minutes', NOW()),
    ('streaming', 756, 756.0, NOW() - INTERVAL '5 minutes', NOW()),
    ('analytics', 642, 642.0, NOW() - INTERVAL '5 minutes', NOW()),
    ('bigdata', 534, 534.0, NOW() - INTERVAL '5 minutes', NOW())
ON CONFLICT (hashtag) DO NOTHING;
