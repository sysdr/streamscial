import logging
import threading
import time
from consumers.robust_consumer import RobustStreamSocialConsumer
from consumers.post_processor import StreamSocialPostProcessor
from monitoring.dashboard import start_dashboard
from producers.test_data_producer import TestDataProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    print("🚀 Starting StreamSocial Error Handling System")
    
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topics = ['streamsocial-posts']
    group_id = 'streamsocial-error-handling-group'
    
    # Initialize components
    post_processor = StreamSocialPostProcessor()
    
    # Start monitoring dashboard in separate thread
    dashboard_thread = threading.Thread(target=start_dashboard, daemon=True)
    dashboard_thread.start()
    print("📊 Dashboard started at http://localhost:8000")
    
    # Start test data producer in separate thread
    def start_producer():
        time.sleep(2)  # Wait for consumer to be ready
        producer = TestDataProducer(bootstrap_servers)
        producer.start_producing(
            'streamsocial-posts', 
            messages_per_second=3, 
            error_rate=0.15
        )
    
    producer_thread = threading.Thread(target=start_producer, daemon=True)
    producer_thread.start()
    print("🎯 Test producer started")
    
    # Initialize robust consumer
    consumer = RobustStreamSocialConsumer(
        topics=topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        message_processor=post_processor.process_social_post
    )
    
    # Start consuming with error handling
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        print(f"📈 Final metrics: {consumer.get_metrics()}")
        print(f"📝 Processed posts: {len(post_processor.get_processed_posts())}")

if __name__ == "__main__":
    main()
