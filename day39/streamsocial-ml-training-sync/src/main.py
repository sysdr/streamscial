import asyncio
import threading
from src.models import init_database, get_session
from src.data_generator import DataGenerator
from src.processors.feature_processor import FeatureProcessor
from src.processors.resolution_processor import ResolutionProcessor
from src.api.monitoring_api import app
import uvicorn
import structlog

logger = structlog.get_logger()

def run_feature_processor():
    processor = FeatureProcessor('localhost:9092')
    processor.process_stream(
        input_topics=[
            'ml-training-user_profiles',
            'ml-training-user_interactions',
            'ml-training-content_metadata'
        ],
        output_topic='ml-training-features'
    )

def run_resolution_processor():
    processor = ResolutionProcessor('localhost:9092')
    processor.process_stream(
        input_topic='ml-training-user_profiles',
        output_topic='ml-resolved-user_profiles',
        record_type='user_profile',
        key_field='user_id'
    )

def main():
    print("Initializing StreamSocial ML Training Data Sync...")
    
    # Initialize database
    init_database()
    
    # Generate test data
    session = get_session()
    generator = DataGenerator()
    generator.generate_user_profiles(session, 100)
    generator.generate_content_metadata(session, 500)
    generator.generate_interactions(session, 1000)
    session.close()
    
    print("Starting monitoring API...")
    uvicorn.run(app, host="0.0.0.0", port=8080)

if __name__ == '__main__':
    main()
