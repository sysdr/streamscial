import time
import statistics
from typing import List, Dict, Any
from serialization_service import SerializationService

class PerformanceBenchmark:
    def __init__(self):
        self.service = SerializationService()
        
    def run_benchmark(self, iterations: int = 1000) -> Dict[str, Any]:
        print(f"Running benchmark with {iterations} iterations...")
        
        formats = {
            'protobuf': (self.service.serialize_protobuf, self.service.deserialize_protobuf),
            'avro': (self.service.serialize_avro, self.service.deserialize_avro),
            'json': (self.service.serialize_json, self.service.deserialize_json)
        }
        
        results = {}
        
        for format_name, (serialize_func, deserialize_func) in formats.items():
            print(f"Benchmarking {format_name}...")
            
            serialization_times = []
            deserialization_times = []
            payload_sizes = []
            
            for i in range(iterations):
                # Create fresh event data for each iteration
                event_data = self.service.create_sample_event()
                
                # Serialize
                serialized_data, ser_time = serialize_func(event_data)
                serialization_times.append(ser_time)
                payload_sizes.append(len(serialized_data))
                
                # Deserialize
                _, deser_time = deserialize_func(serialized_data)
                deserialization_times.append(deser_time)
            
            results[format_name] = {
                'avg_serialization_time_ns': statistics.mean(serialization_times),
                'avg_deserialization_time_ns': statistics.mean(deserialization_times),
                'avg_payload_size_bytes': statistics.mean(payload_sizes),
                'min_payload_size_bytes': min(payload_sizes),
                'max_payload_size_bytes': max(payload_sizes),
                'total_time_ns': statistics.mean(serialization_times) + statistics.mean(deserialization_times)
            }
        
        return results
    
    def print_results(self, results: Dict[str, Any]):
        print("\n" + "="*80)
        print("PERFORMANCE BENCHMARK RESULTS")
        print("="*80)
        
        baseline_format = 'json'
        baseline_size = results[baseline_format]['avg_payload_size_bytes']
        baseline_time = results[baseline_format]['total_time_ns']
        
        for format_name, metrics in results.items():
            size_reduction = ((baseline_size - metrics['avg_payload_size_bytes']) / baseline_size) * 100
            speed_improvement = ((baseline_time - metrics['total_time_ns']) / baseline_time) * 100
            
            print(f"\n{format_name.upper()}:")
            print(f"  Avg Payload Size: {metrics['avg_payload_size_bytes']:.1f} bytes")
            print(f"  Size vs JSON: {size_reduction:+.1f}%")
            print(f"  Avg Ser Time: {metrics['avg_serialization_time_ns']/1000:.1f} μs")
            print(f"  Avg Deser Time: {metrics['avg_deserialization_time_ns']/1000:.1f} μs")
            print(f"  Total Time: {metrics['total_time_ns']/1000:.1f} μs")
            print(f"  Speed vs JSON: {speed_improvement:+.1f}%")
        
        print("\n" + "="*80)

if __name__ == "__main__":
    benchmark = PerformanceBenchmark()
    results = benchmark.run_benchmark(1000)
    benchmark.print_results(results)
