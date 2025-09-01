import time
import statistics
import threading
import sys
import os
from concurrent.futures import ThreadPoolExecutor

# Add the src directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producers.acks_producer import AcksProducerManager
from events.event_types import StreamSocialEvent, EventType

class ProducerBenchmark:
    def __init__(self):
        self.producer_manager = AcksProducerManager()
        self.results = {}
    
    def benchmark_acks_strategy(self, event_type: EventType, num_events: int = 1000, num_threads: int = 5):
        """Benchmark a specific acknowledgment strategy"""
        print(f"\n🔬 Benchmarking {event_type.value} events ({num_events} events, {num_threads} threads)")
        
        events = []
        for i in range(num_events):
            if event_type == EventType.PAYMENT_PROCESSING:
                event = StreamSocialEvent.create_critical_event(
                    event_type, f"user_{i}", {"amount": 1000 + i}
                )
            elif event_type == EventType.POST_CREATION:
                event = StreamSocialEvent.create_social_event(
                    event_type, f"user_{i}", {"post_id": f"post_{i}"}
                )
            else:
                event = StreamSocialEvent.create_analytics_event(
                    event_type, f"user_{i}", {"page": "/feed"}
                )
            events.append(event)
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(self.producer_manager.send_event, event) for event in events]
            results = [future.result() for future in futures]
        
        # Flush to ensure all messages are processed
        self.producer_manager.flush_all()
        end_time = time.time()
        
        total_time = end_time - start_time
        throughput = num_events / total_time
        
        # Get metrics for this event type
        producer_type = self.producer_manager._get_producer_type(event_type)
        metrics = self.producer_manager.get_metrics()[producer_type]
        
        avg_latency = statistics.mean(metrics['latency']) if metrics['latency'] else 0
        p95_latency = statistics.quantiles(metrics['latency'], n=20)[18] if len(metrics['latency']) > 20 else avg_latency
        success_rate = (metrics['acked'] / metrics['sent']) * 100 if metrics['sent'] > 0 else 0
        
        result = {
            'event_type': event_type.value,
            'producer_type': producer_type,
            'total_time': total_time,
            'throughput': throughput,
            'avg_latency': avg_latency,
            'p95_latency': p95_latency,
            'success_rate': success_rate,
            'total_sent': metrics['sent'],
            'total_acked': metrics['acked'],
            'total_failed': metrics['failed']
        }
        
        self.results[event_type.value] = result
        return result
    
    def run_comprehensive_benchmark(self):
        """Run comprehensive benchmark across all acknowledgment strategies"""
        print("🏁 Starting StreamSocial Producer Acknowledgment Benchmark")
        print("=" * 60)
        
        test_cases = [
            (EventType.PAYMENT_PROCESSING, 500),  # Critical events
            (EventType.POST_CREATION, 1000),     # Social events  
            (EventType.PAGE_VIEW, 2000)          # Analytics events
        ]
        
        for event_type, num_events in test_cases:
            result = self.benchmark_acks_strategy(event_type, num_events)
            
            print(f"📊 Results for {result['event_type']} ({result['producer_type']} producer):")
            print(f"   Throughput: {result['throughput']:.2f} events/sec")
            print(f"   Avg Latency: {result['avg_latency']:.2f}ms")
            print(f"   P95 Latency: {result['p95_latency']:.2f}ms")
            print(f"   Success Rate: {result['success_rate']:.2f}%")
            print(f"   Total: {result['total_sent']} sent, {result['total_acked']} acked, {result['total_failed']} failed")
        
        self.print_comparison()
    
    def print_comparison(self):
        """Print comparison table of all acknowledgment strategies"""
        print("\n📈 Acknowledgment Strategy Comparison")
        print("=" * 80)
        print(f"{'Strategy':<20} {'Throughput':<15} {'Avg Latency':<15} {'Reliability':<15} {'Use Case':<15}")
        print("-" * 80)
        
        strategy_mapping = {
            'payment_processing': ('acks=all', 'Critical Data'),
            'post_creation': ('acks=1', 'Social Content'),
            'page_view': ('acks=0', 'Analytics')
        }
        
        for event_type, result in self.results.items():
            strategy, use_case = strategy_mapping.get(event_type, ('Unknown', 'Unknown'))
            print(f"{strategy:<20} {result['throughput']:<15.2f} {result['avg_latency']:<15.2f} {result['success_rate']:<15.2f}% {use_case:<15}")
        
        print("\n💡 Key Insights:")
        print("   • acks=all: Highest reliability, lowest throughput")
        print("   • acks=1: Balanced reliability and performance")  
        print("   • acks=0: Highest throughput, no delivery guarantee")
    
    def cleanup(self):
        self.producer_manager.close_all()

if __name__ == "__main__":
    benchmark = ProducerBenchmark()
    try:
        benchmark.run_comprehensive_benchmark()
    finally:
        benchmark.cleanup()
