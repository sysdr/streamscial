import time
import random
import threading
from datetime import datetime
from producer.high_volume_producer import create_high_volume_producer, UserAction
from monitoring.metrics_server import MetricsServer
from monitoring.dashboard import ProducerDashboard
import multiprocessing

class StreamSocialDemo:
    def __init__(self):
        self.producer = create_high_volume_producer(
            bootstrap_servers="localhost:9092",
            pool_size=20,
            worker_threads=16
        )
        self.running = False
        self.demo_threads = []

    def generate_user_actions(self, rate_per_second=1000, duration=60):
        """Generate user actions at specified rate"""
        print(f"📱 Generating user actions at {rate_per_second}/sec for {duration} seconds")
        
        action_types = ['post', 'like', 'share', 'comment']
        
        end_time = time.time() + duration
        interval = 1.0 / rate_per_second
        
        while time.time() < end_time and self.running:
            start_batch = time.time()
            
            # Generate batch of actions
            batch_size = min(100, rate_per_second // 10)  # Batch size based on rate
            actions = []
            
            for _ in range(batch_size):
                action = UserAction(
                    user_id=f"user_{random.randint(1, 100000)}",
                    action_type=random.choice(action_types),
                    content_id=f"content_{random.randint(1, 1000000)}",
                    timestamp=int(time.time() * 1000),
                    metadata={
                        'device': random.choice(['mobile', 'web', 'tablet']),
                        'location': random.choice(['US', 'EU', 'ASIA']),
                        'session_id': f"session_{random.randint(1, 50000)}"
                    }
                )
                actions.append(action)
            
            # Send batch
            self.producer.send_batch('user-actions', actions)
            
            # Maintain rate
            elapsed = time.time() - start_batch
            sleep_time = (batch_size * interval) - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    def stress_test(self):
        """Run stress test with increasing load"""
        print("🚀 Starting stress test...")
        
        test_phases = [
            (1000, 30, "Warmup"),
            (10000, 60, "Normal Load"),
            (50000, 30, "High Load"),
            (100000, 15, "Peak Load"),
            (5000000, 10, "Ultra High Load"),  # 5M/sec target
            (10000, 30, "Cooldown")
        ]
        
        for rate, duration, phase_name in test_phases:
            if not self.running:
                break
                
            print(f"📈 Phase: {phase_name} - {rate:,} messages/sec for {duration}s")
            
            # Start multiple generators for high rates
            generators = min(8, max(1, rate // 10000))  # Scale generators with rate
            rate_per_generator = rate // generators
            
            threads = []
            for i in range(generators):
                thread = threading.Thread(
                    target=self.generate_user_actions,
                    args=(rate_per_generator, duration),
                    name=f"Generator-{i}"
                )
                thread.start()
                threads.append(thread)
            
            # Wait for phase to complete
            for thread in threads:
                thread.join()
            
            print(f"✅ Completed phase: {phase_name}")
            time.sleep(5)  # Brief pause between phases

    def start_demo(self):
        """Start the demo with all components"""
        print("🎬 Starting StreamSocial Producer Demo")
        
        # Start producer
        self.producer.start()
        self.running = True
        
        # Start metrics server in separate process
        metrics_server = MetricsServer(self.producer)
        metrics_process = multiprocessing.Process(
            target=metrics_server.run,
            kwargs={'host': '0.0.0.0', 'debug': False}
        )
        metrics_process.start()
        
        # Start dashboard in separate process  
        dashboard = ProducerDashboard()
        dashboard_process = multiprocessing.Process(
            target=dashboard.run,
            kwargs={'host': '0.0.0.0', 'debug': False}
        )
        dashboard_process.start()
        
        print("🚀 All services started!")
        print("📊 Dashboard: http://localhost:8050")
        print("📈 Metrics API: http://localhost:8080/metrics")
        print("💡 Starting stress test in 10 seconds...")
        
        time.sleep(10)
        
        # Run stress test
        stress_thread = threading.Thread(target=self.stress_test, name="StressTest")
        stress_thread.start()
        
        try:
            stress_thread.join()
        except KeyboardInterrupt:
            print("\n🛑 Demo interrupted by user")
        
        # Cleanup
        self.stop_demo()
        metrics_process.terminate()
        dashboard_process.terminate()

    def stop_demo(self):
        """Stop the demo"""
        print("🛑 Stopping demo...")
        self.running = False
        self.producer.stop()
        print("✅ Demo stopped")

if __name__ == '__main__':
    demo = StreamSocialDemo()
    demo.start_demo()
