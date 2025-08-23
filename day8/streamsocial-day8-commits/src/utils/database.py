from sqlalchemy import create_engine, Column, String, DateTime, Boolean, JSON, Integer
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import structlog

logger = structlog.get_logger()

Base = declarative_base()

class ProcessedEngagement(Base):
    __tablename__ = 'processed_engagements'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String, unique=True, index=True)
    user_id = Column(String, index=True)
    content_id = Column(String, index=True)
    engagement_type = Column(String)
    original_timestamp = Column(DateTime)
    processed_timestamp = Column(DateTime, default=datetime.utcnow)
    event_metadata = Column(JSON)

class DatabaseManager:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database initialized")
    
    def get_session(self):
        return self.SessionLocal()
    
    def store_engagement(self, engagement_event, session=None):
        """Store engagement with transaction support"""
        close_session = session is None
        if session is None:
            session = self.get_session()
        
        try:
            processed_engagement = ProcessedEngagement(
                event_id=engagement_event.event_id,
                user_id=engagement_event.user_id,
                content_id=engagement_event.content_id,
                engagement_type=engagement_event.engagement_type,
                original_timestamp=engagement_event.timestamp,
                event_metadata=engagement_event.metadata
            )
            session.add(processed_engagement)
            
            if close_session:
                session.commit()
                logger.info("Engagement stored", event_id=engagement_event.event_id)
            
            return True
        except Exception as e:
            if close_session:
                session.rollback()
            logger.error("Failed to store engagement", error=str(e))
            raise
        finally:
            if close_session:
                session.close()
    
    def is_duplicate(self, event_id: str) -> bool:
        """Check if engagement already processed (idempotency)"""
        session = self.get_session()
        try:
            result = session.query(ProcessedEngagement).filter_by(event_id=event_id).first()
            return result is not None
        finally:
            session.close()
