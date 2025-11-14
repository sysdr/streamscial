-- Create user_profiles table
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    full_name VARCHAR(255),
    bio TEXT,
    location VARCHAR(255),
    website VARCHAR(255),
    avatar_url VARCHAR(512),
    is_verified BOOLEAN DEFAULT FALSE,
    follower_count INT DEFAULT 0,
    following_count INT DEFAULT 0,
    post_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX idx_username ON user_profiles(username);
CREATE INDEX idx_email ON user_profiles(email);
CREATE INDEX idx_updated_at ON user_profiles(updated_at);

-- Create trigger to update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_user_profiles_updated_at 
    BEFORE UPDATE ON user_profiles 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Insert sample data
INSERT INTO user_profiles (username, email, full_name, bio, location, is_verified, follower_count, following_count, post_count)
VALUES 
    ('tech_guru', 'guru@streamsocial.com', 'Tech Guru', 'Software architect passionate about distributed systems', 'San Francisco, CA', TRUE, 15000, 500, 1200),
    ('data_scientist', 'data@streamsocial.com', 'Data Scientist', 'ML engineer exploring real-time analytics', 'New York, NY', TRUE, 8500, 300, 850),
    ('cloud_native', 'cloud@streamsocial.com', 'Cloud Native Dev', 'Building scalable cloud applications', 'Seattle, WA', FALSE, 4200, 450, 620),
    ('kafka_enthusiast', 'kafka@streamsocial.com', 'Kafka Fan', 'Event streaming evangelist', 'Austin, TX', FALSE, 3100, 280, 490)
ON CONFLICT (username) DO NOTHING;
