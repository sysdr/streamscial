-- StreamSocial Content Database Schema

CREATE TABLE IF NOT EXISTS user_posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    media_url VARCHAR(512),
    post_type VARCHAR(50) DEFAULT 'text',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_comments (
    id SERIAL PRIMARY KEY,
    post_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    comment_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_uploads (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    media_type VARCHAR(50),
    storage_path VARCHAR(512),
    file_size BIGINT,
    upload_status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO user_posts (user_id, content, post_type) VALUES
    (1001, 'Exploring the mountains this weekend! #adventure', 'text'),
    (1002, 'Just finished reading an amazing book about AI', 'text'),
    (1003, 'Check out my new photography portfolio', 'media'),
    (1004, 'Cooking tips for beginners: Start with simple recipes', 'text'),
    (1005, 'Morning workout complete! Feeling energized', 'text');

INSERT INTO user_comments (post_id, user_id, comment_text) VALUES
    (1, 2001, 'That looks amazing! Which trail did you take?'),
    (2, 2002, 'I love that book! The AI ethics chapter was fascinating'),
    (3, 2003, 'Stunning photos! What camera do you use?');

INSERT INTO media_uploads (user_id, media_type, storage_path, file_size, upload_status) VALUES
    (1001, 'image/jpeg', '/cdn/images/mountain_view_001.jpg', 2048576, 'completed'),
    (1003, 'image/png', '/cdn/images/portfolio_header.png', 3145728, 'completed'),
    (1005, 'video/mp4', '/cdn/videos/workout_routine.mp4', 10485760, 'processing');

-- Create function to simulate continuous data generation
CREATE OR REPLACE FUNCTION generate_post() RETURNS void AS $$
DECLARE
    random_user INTEGER;
    random_content TEXT;
BEGIN
    random_user := 1000 + floor(random() * 100)::INTEGER;
    random_content := 'Auto-generated post at ' || NOW()::TEXT;
    
    INSERT INTO user_posts (user_id, content, post_type)
    VALUES (random_user, random_content, 'text');
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;
