import time
import json
import statistics
from typing import List, Dict, Any
from producers.avro_producer import StreamSocialAvroProducer
from streamsocial.models import UserProfile, UserInteraction, PostEvent
from confluent_kafka.serialization import SerializationContext, MessageField

class PerformanceBenchmark:
    def __init__(self):
        self.producer = StreamSocialAvroProducer()
        self.results = {
            'avro_times': [],
            'json_times': [],
            'avro_sizes': [],
            'json_sizes': []
        }
    
    def benchmark_serialization(self, iterations=1000):
        """Benchmark Avro vs JSON serialization"""
        print(f"🚀 Starting serialization benchmark ({iterations} iterations)")
        
        # Sample data
        profile = UserProfile("user123", "johndoe", "john@example.com")
        interaction = UserInteraction("int456", "user123", "LIKE", "post789")
        post = PostEvent("post789", "user123", "Hello StreamSocial! #kafka #avro", 
                        hashtags=["kafka", "avro"], mentions=["@alice"])
        
        for i in range(iterations):
            # Benchmark Avro
            start_time = time.perf_counter()
            ctx = SerializationContext("test-topic", MessageField.VALUE)
            avro_data = self.producer.serializers['user_profile'](profile.to_dict(), ctx)
            avro_time = time.perf_counter() - start_time
            
            # Benchmark JSON
            start_time = time.perf_counter()
            json_data = json.dumps(profile.to_dict()).encode('utf-8')
            json_time = time.perf_counter() - start_time
            
            self.results['avro_times'].append(avro_time * 1000)  # Convert to ms
            self.results['json_times'].append(json_time * 1000)
            self.results['avro_sizes'].append(len(avro_data))
            self.results['json_sizes'].append(len(json_data))
        
        self._print_results()
    
    def _print_results(self):
        """Print benchmark results"""
        print("\n📊 Performance Benchmark Results")
        print("=" * 50)
        
        # Serialization time comparison
        avg_avro_time = statistics.mean(self.results['avro_times'])
        avg_json_time = statistics.mean(self.results['json_times'])
        time_improvement = ((avg_json_time - avg_avro_time) / avg_json_time) * 100
        
        print(f"⏱️ Serialization Time:")
        print(f"   Avro: {avg_avro_time:.3f}ms (avg)")
        print(f"   JSON: {avg_json_time:.3f}ms (avg)")
        print(f"   Improvement: {time_improvement:.1f}% faster")
        
        # Size comparison
        avg_avro_size = statistics.mean(self.results['avro_sizes'])
        avg_json_size = statistics.mean(self.results['json_sizes'])
        size_reduction = ((avg_json_size - avg_avro_size) / avg_json_size) * 100
        
        print(f"\n📦 Payload Size:")
        print(f"   Avro: {avg_avro_size:.0f} bytes (avg)")
        print(f"   JSON: {avg_json_size:.0f} bytes (avg)")
        print(f"   Reduction: {size_reduction:.1f}% smaller")
        
        print(f"\n🎯 Summary:")
        print(f"   • Avro is {time_improvement:.1f}% faster to serialize")
        print(f"   • Avro payloads are {size_reduction:.1f}% smaller")
        print(f"   • Network bandwidth savings: {size_reduction:.1f}%")
