import sys
import time
import signal
import threading
from producers.acks_producer import AcksProducerManager
from monitoring.dashboard import MetricsDashboard
from events.simulator import StreamSocialEventSimulator

def signal_handler(signum, frame):
    print("\nShutting down StreamSocial Producer Demo...")
    sys.exit(0)

def main():
    print("🚀 Starting StreamSocial Producer Acknowledgment Demo")
    print("=" * 50)
    
    # Initialize components
    producer_manager = AcksProducerManager()
    dashboard = MetricsDashboard(producer_manager)
    simulator = StreamSocialEventSimulator(producer_manager)
    
    # Setup graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start event simulation
        simulator.start(events_per_second=20)
        
        print("📊 Dashboard available at: http://localhost:5000")
        print("🔄 Generating 20 events per second...")
        print("💡 Observe different acknowledgment behaviors:")
        print("   - Critical events (red): acks=all - Highest durability")
        print("   - Social events (blue): acks=1 - Balanced approach") 
        print("   - Analytics events (green): acks=0 - Highest throughput")
        print("\nPress Ctrl+C to stop...")
        
        # Start dashboard (blocking)
        dashboard.run(debug=False)
        
    except KeyboardInterrupt:
        print("\n🛑 Received shutdown signal")
    finally:
        simulator.stop()
        producer_manager.flush_all()
        producer_manager.close_all()
        print("✅ StreamSocial Producer Demo stopped successfully")

if __name__ == "__main__":
    main()
