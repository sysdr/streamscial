import logging
import signal
import sys
import time
import threading
from multiprocessing import Process
from src.partition_manager import PartitionAssignmentManager
from src.trend_worker import TrendWorker
from src.post_producer import SocialPostProducer
from config.kafka_config import KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamSocialTrendSystem:
    def __init__(self):
        self.partition_manager = PartitionAssignmentManager()
        self.workers = []
        self.producer = None
        self.running = False
        
    def setup_system(self):
        """Initialize the system"""
        logger.info("Setting up StreamSocial Trend Analysis System")
        
        # Initialize Kafka topics
        self.partition_manager.initialize_topics()
        
        # Calculate and store partition assignments
        assignments = self.partition_manager.calculate_partition_assignment(
            KafkaConfig.WORKER_COUNT
        )
        self.partition_manager.store_assignment(assignments)
        
        logger.info(f"Partition assignments: {assignments}")
    
    def start_workers(self):
        """Start trend analysis workers"""
        logger.info("Starting trend workers...")
        
        for worker_id in range(KafkaConfig.WORKER_COUNT):
            worker = TrendWorker(worker_id)
            worker_process = Process(target=worker.start)
            worker_process.start()
            self.workers.append(worker_process)
            
        logger.info(f"Started {len(self.workers)} trend workers")
    
    def start_producer(self):
        """Start social post producer for testing"""
        logger.info("Starting social post producer...")
        self.producer = SocialPostProducer()
        self.producer.start_producing(posts_per_second=10)
    
    def stop_system(self):
        """Stop all components"""
        logger.info("Stopping system...")
        self.running = False
        
        if self.producer:
            self.producer.stop_producing()
            
        for worker in self.workers:
            worker.terminate()
            worker.join()
            
        logger.info("System stopped")
    
    def run(self):
        """Run the complete system"""
        def signal_handler(sig, frame):
            logger.info("Received shutdown signal")
            self.stop_system()
            sys.exit(0)
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            self.setup_system()
            time.sleep(2)  # Allow topics to be created
            
            self.start_workers()
            time.sleep(3)  # Allow workers to initialize
            
            self.start_producer()
            
            self.running = True
            logger.info("System running. Press Ctrl+C to stop.")
            
            while self.running:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"System error: {e}")
            self.stop_system()

if __name__ == "__main__":
    system = StreamSocialTrendSystem()
    system.run()
