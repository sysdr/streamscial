import asyncio
import signal
import sys
from config.app_config import AppConfig
from src.engagement_processor.processor import EngagementProcessor

class StreamSocialOffsetManager:
    def __init__(self):
        self.config = AppConfig()
        self.processor = None
        self.running = True
        
    async def start(self):
        """Start the offset management system"""
        print("🚀 Starting StreamSocial Offset Management System")
        
        # Initialize processor
        self.processor = EngagementProcessor(self.config)
        await self.processor.initialize()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            # Start processing
            await self.processor.start_processing()
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the system gracefully"""
        print("🛑 Stopping StreamSocial Offset Management System")
        if self.processor:
            await self.processor.stop()
        self.running = False
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n📡 Received signal {signum}")
        self.running = False

if __name__ == "__main__":
    app = StreamSocialOffsetManager()
    try:
        asyncio.run(app.start())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
