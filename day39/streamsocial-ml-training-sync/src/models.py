from sqlalchemy import Column, BigInteger, String, Integer, Float, DateTime, JSON, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os

Base = declarative_base()

class UserProfile(Base):
    __tablename__ = 'user_profiles'
    
    user_id = Column(BigInteger, primary_key=True)
    username = Column(String(50), nullable=False)
    email = Column(String(100))
    interests = Column(JSON, default=list)
    follower_count = Column(Integer, default=0)
    following_count = Column(Integer, default=0)
    account_age_days = Column(Integer, default=0)
    engagement_rate = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class UserInteraction(Base):
    __tablename__ = 'user_interactions'
    
    interaction_id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    content_id = Column(BigInteger, nullable=False)
    interaction_type = Column(String(20), nullable=False)
    duration_seconds = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class ContentMetadata(Base):
    __tablename__ = 'content_metadata'
    
    content_id = Column(BigInteger, primary_key=True)
    creator_id = Column(BigInteger, nullable=False)
    title = Column(String(200))
    category = Column(String(50))
    tags = Column(JSON, default=list)
    engagement_score = Column(Float, default=0.0)
    view_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class FeatureVector(Base):
    __tablename__ = 'feature_vectors'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, nullable=False)
    content_id = Column(BigInteger)
    features = Column(JSON, nullable=False)
    version = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)

def get_engine():
    db_url = os.getenv('DATABASE_URL', 'postgresql://streamsocial:streamsocial@localhost:5433/streamsocial')
    return create_engine(db_url)

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

def init_database():
    engine = get_engine()
    Base.metadata.create_all(engine)
    
    # Create timestamp trigger function
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE OR REPLACE FUNCTION update_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """))
        
        # Create triggers for timestamp auto-update
        for table in ['user_profiles', 'content_metadata']:
            conn.execute(text(f"""
                DROP TRIGGER IF EXISTS {table}_timestamp ON {table};
                CREATE TRIGGER {table}_timestamp
                BEFORE UPDATE ON {table}
                FOR EACH ROW EXECUTE FUNCTION update_timestamp();
            """))
        
        # Create indexes for efficient sync
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_user_profiles_updated 
            ON user_profiles (updated_at);
            
            CREATE INDEX IF NOT EXISTS idx_user_interactions_sync 
            ON user_interactions (interaction_id, created_at);
            
            CREATE INDEX IF NOT EXISTS idx_content_metadata_updated 
            ON content_metadata (updated_at);
        """))
        conn.commit()
    
    return engine
