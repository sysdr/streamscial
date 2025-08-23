import asyncio
import argparse
import signal
import sys
from typing import Dict, Any
import structlog

from .utils.database import DatabaseManager
from .monitoring.metrics import MetricsCollector
from .monitoring.dashboard import MonitoringDashboard
from .consumers.engagement_consumer import ReliableEngagementConsumer
from .producers.engagement_producer import EngagementProducer

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

class StreamSocialApp:
    def __init__(self):
        self.config = self.load_config()
        self.db_manager = None
        self.metrics = None
        self.dashboard = None
        self.consumer = None
        self.producer = None
        self.running = False
    
    def load_config(self) -> Dict[str, Any]:
        """Load application configuration"""
        return {
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topic': 'user-engagements',
                'group_id': 'engagement-processors',
                'batch_size': 50,
                'max_processing_time': 30
            },
            'database': {
                'connection_string': 'postgresql://admin:password123@localhost:5432/streamsocial'
            },
            'monitoring': {
                'dashboard_host': '127.0.0.1',
                'dashboard_port': 8000
            }
        }
    
    async def initialize(self):
        """Initialize all components"""
        logger.info("Initializing StreamSocial Engagement Pipeline")
        
        # Initialize database
        self.db_manager = DatabaseManager(self.config['database']['connection_string'])
        
        # Initialize metrics
        self.metrics = MetricsCollector()
        
        # Initialize dashboard
        self.dashboard = MonitoringDashboard(self.metrics)
        
        # Initialize consumer
        self.consumer = ReliableEngagementConsumer(
            self.config['kafka'], 
            self.db_manager, 
            self.metrics
        )
        
        # Initialize producer for testing
        self.producer = EngagementProducer(self.config['kafka'])
        
        logger.info("All components initialized successfully")
    
    async def start_consumer(self):
        """Start the engagement consumer"""
        logger.info("Starting engagement consumer")
        await self.consumer.start()
    
    async def start_dashboard(self):
        """Start the monitoring dashboard"""
        logger.info("Starting monitoring dashboard")
        await self.dashboard.start(
            host=self.config['monitoring']['dashboard_host'],
            port=self.config['monitoring']['dashboard_port']
        )
    
    async def produce_test_data(self, count: int = 100):
        """Produce test engagement data"""
        logger.info(f"Producing {count} test engagements")
        await self.producer.produce_sample_data(count)
    
    async def run_consumer_mode(self):
        """Run in consumer mode"""
        await self.initialize()
        
        # Start dashboard in background
        dashboard_task = asyncio.create_task(self.start_dashboard())
        
        # Start consumer
        try:
            await self.start_consumer()
        except KeyboardInterrupt:
            logger.info("Shutting down consumer")
        finally:
            dashboard_task.cancel()
    
    async def run_producer_mode(self, count: int):
        """Run in producer mode"""
        await self.initialize()
        await self.produce_test_data(count)
    
    async def run_full_demo(self):
        """Run full demonstration"""
        await self.initialize()
        
        # Start dashboard
        dashboard_task = asyncio.create_task(self.start_dashboard())
        
        # Start consumer
        consumer_task = asyncio.create_task(self.start_consumer())
        
        # Wait a bit for consumer to start
        await asyncio.sleep(2)
        
        # Start producing test data
        producer_task = asyncio.create_task(self.produce_test_data(200))
        
        try:
            await asyncio.gather(consumer_task, producer_task, dashboard_task)
        except KeyboardInterrupt:
            logger.info("Demo interrupted, shutting down")
        finally:
            # Cancel all tasks
            for task in [dashboard_task, consumer_task, producer_task]:
                task.cancel()

async def main():
    parser = argparse.ArgumentParser(description='StreamSocial Engagement Pipeline')
    parser.add_argument('--mode', choices=['consumer', 'producer', 'demo'], 
                       default='demo', help='Run mode')
    parser.add_argument('--count', type=int, default=100, 
                       help='Number of test messages to produce')
    
    args = parser.parse_args()
    
    app = StreamSocialApp()
    
    try:
        if args.mode == 'consumer':
            await app.run_consumer_mode()
        elif args.mode == 'producer':
            await app.run_producer_mode(args.count)
        else:
            await app.run_full_demo()
    except Exception as e:
        logger.error("Application error", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
