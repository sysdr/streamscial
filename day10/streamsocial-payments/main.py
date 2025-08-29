#!/usr/bin/env python3
"""
StreamSocial Payment Processing System
Demonstrates exactly-once delivery guarantees with Kafka
"""

import asyncio
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
import structlog
import uvicorn
from src.web.app import create_app
from src.consumers.payment_consumer import PaymentConsumer
from src.producers.payment_producer import PaymentProducer

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

logger = structlog.get_logger(__name__)

class PaymentProcessingSystem:
    def __init__(self):
        self.consumer = None
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.running = False
        
    def start_consumer(self):
        """Start the payment consumer in a separate thread"""
        try:
            self.consumer = PaymentConsumer("localhost:9092")
            self.consumer.subscribe_and_consume(["payment-events"])
        except Exception as e:
            logger.error("Consumer startup failed", error=str(e))
    
    async def start(self):
        """Start the entire payment processing system"""
        logger.info("Starting StreamSocial Payment Processing System")
        
        # Start consumer in background thread
        self.running = True
        consumer_future = self.executor.submit(self.start_consumer)
        
        # Start web application
        app = create_app()
        config = uvicorn.Config(
            app=app,
            host="0.0.0.0",
            port=8080,
            log_level="info",
            access_log=True
        )
        
        server = uvicorn.Server(config)
        
        # Handle shutdown gracefully
        def signal_handler(signum, frame):
            logger.info("Received shutdown signal")
            self.running = False
            if self.consumer:
                self.consumer.close()
            self.executor.shutdown(wait=True)
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start server
        await server.serve()

async def main():
    """Main entry point"""
    system = PaymentProcessingSystem()
    await system.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error("Application failed", error=str(e))
        sys.exit(1)
