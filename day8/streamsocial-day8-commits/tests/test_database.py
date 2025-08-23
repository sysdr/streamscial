import pytest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.utils.database import DatabaseManager, ProcessedEngagement, Base
from src.models.engagement import EngagementEvent

@pytest.fixture
def in_memory_db():
    # Create in-memory SQLite database for testing
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    
    db_manager = DatabaseManager("sqlite:///:memory:")
    db_manager.engine = engine
    db_manager.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    return db_manager

@pytest.fixture
def sample_engagement():
    return EngagementEvent(
        user_id="test_user",
        content_id="test_content",
        engagement_type="like",
        timestamp=datetime.utcnow(),
        metadata={"device": "mobile"},
        event_id="test_event_123"
    )

class TestDatabaseManager:
    def test_store_engagement_success(self, in_memory_db, sample_engagement):
        result = in_memory_db.store_engagement(sample_engagement)
        
        assert result is True
        
        # Verify the engagement was stored
        session = in_memory_db.get_session()
        stored = session.query(ProcessedEngagement).filter_by(event_id=sample_engagement.event_id).first()
        session.close()
        
        assert stored is not None
        assert stored.user_id == sample_engagement.user_id
        assert stored.content_id == sample_engagement.content_id
    
    def test_is_duplicate_false(self, in_memory_db):
        result = in_memory_db.is_duplicate("non_existent_id")
        assert result is False
    
    def test_is_duplicate_true(self, in_memory_db, sample_engagement):
        # Store the engagement first
        in_memory_db.store_engagement(sample_engagement)
        
        # Check if it's a duplicate
        result = in_memory_db.is_duplicate(sample_engagement.event_id)
        assert result is True

if __name__ == "__main__":
    pytest.main([__file__])
