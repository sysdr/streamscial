import gzip
import snappy
import lz4.frame
import zstandard as zstd
import time
import json
import statistics
from typing import Dict, List, Any
import psutil
import threading

class CompressionAnalyzer:
    def __init__(self):
        self.algorithms = {
            'gzip': self._gzip_compress,
            'snappy': self._snappy_compress,
            'lz4': self._lz4_compress,
            'zstd': self._zstd_compress
        }
        
        self.decompressors = {
            'gzip': self._gzip_decompress,
            'snappy': self._snappy_decompress,
            'lz4': self._lz4_decompress,
            'zstd': self._zstd_decompress
        }
        
        # ZSTD dictionary for better compression
        self.zstd_dict = None
        
    def _gzip_compress(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=6)
    
    def _gzip_decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)
    
    def _snappy_compress(self, data: bytes) -> bytes:
        return snappy.compress(data)
    
    def _snappy_decompress(self, data: bytes) -> bytes:
        return snappy.uncompress(data)
    
    def _lz4_compress(self, data: bytes) -> bytes:
        return lz4.frame.compress(data)
    
    def _lz4_decompress(self, data: bytes) -> bytes:
        return lz4.frame.decompress(data)
    
    def _zstd_compress(self, data: bytes) -> bytes:
        cctx = zstd.ZstdCompressor(level=3)
        return cctx.compress(data)
    
    def _zstd_decompress(self, data: bytes) -> bytes:
        dctx = zstd.ZstdDecompressor()
        return dctx.decompress(data)
    
    def compress_data(self, data: bytes, algorithm: str) -> bytes:
        if algorithm not in self.algorithms:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        return self.algorithms[algorithm](data)
    
    def decompress_data(self, data: bytes, algorithm: str) -> bytes:
        if algorithm not in self.decompressors:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        return self.decompressors[algorithm](data)
    
    def benchmark_algorithm(self, data: bytes, algorithm: str, iterations: int = 100) -> Dict[str, Any]:
        compression_times = []
        decompression_times = []
        compressed_sizes = []
        cpu_usage_samples = []
        
        process = psutil.Process()
        
        for i in range(iterations):
            # Measure CPU before compression
            cpu_before = process.cpu_percent()
            
            # Compression benchmark
            start_time = time.perf_counter()
            compressed = self.compress_data(data, algorithm)
            compression_time = time.perf_counter() - start_time
            
            # Decompression benchmark
            start_time = time.perf_counter()
            decompressed = self.decompress_data(compressed, algorithm)
            decompression_time = time.perf_counter() - start_time
            
            # Measure CPU after operations
            cpu_after = process.cpu_percent()
            
            # Verify data integrity
            if decompressed != data:
                raise RuntimeError(f"Data corruption detected in {algorithm}")
            
            compression_times.append(compression_time * 1000)  # Convert to ms
            decompression_times.append(decompression_time * 1000)
            compressed_sizes.append(len(compressed))
            cpu_usage_samples.append(max(cpu_after - cpu_before, 0))
        
        original_size = len(data)
        avg_compressed_size = statistics.mean(compressed_sizes)
        compression_ratio = original_size / avg_compressed_size if avg_compressed_size > 0 else 0
        
        return {
            'algorithm': algorithm,
            'original_size': original_size,
            'compressed_size': avg_compressed_size,
            'compression_ratio': compression_ratio,
            'compression_time_ms': {
                'mean': statistics.mean(compression_times),
                'median': statistics.median(compression_times),
                'min': min(compression_times),
                'max': max(compression_times),
                'stdev': statistics.stdev(compression_times) if len(compression_times) > 1 else 0
            },
            'decompression_time_ms': {
                'mean': statistics.mean(decompression_times),
                'median': statistics.median(decompression_times),
                'min': min(decompression_times),
                'max': max(decompression_times),
                'stdev': statistics.stdev(decompression_times) if len(decompression_times) > 1 else 0
            },
            'cpu_usage_delta': statistics.mean(cpu_usage_samples),
            'throughput_mb_per_sec': (original_size / 1024 / 1024) / (statistics.mean(compression_times) / 1000),
            'space_savings_percent': (1 - avg_compressed_size / original_size) * 100
        }
    
    def benchmark_all_algorithms(self, data: bytes, iterations: int = 100) -> Dict[str, Any]:
        results = {}
        
        for algorithm in self.algorithms.keys():
            try:
                results[algorithm] = self.benchmark_algorithm(data, algorithm, iterations)
            except Exception as e:
                results[algorithm] = {'error': str(e)}
        
        # Add comparison insights
        results['insights'] = self._generate_insights(results)
        return results
    
    def _generate_insights(self, results: Dict[str, Any]) -> Dict[str, str]:
        insights = {}
        
        # Find best compression ratio
        best_ratio = max(
            [(k, v.get('compression_ratio', 0)) for k, v in results.items() if 'error' not in v],
            key=lambda x: x[1], default=(None, 0)
        )
        if best_ratio[0]:
            insights['best_compression'] = f"{best_ratio[0]} achieves {best_ratio[1]:.2f}x compression ratio"
        
        # Find fastest compression
        fastest_compression = min(
            [(k, v.get('compression_time_ms', {}).get('mean', float('inf'))) 
             for k, v in results.items() if 'error' not in v],
            key=lambda x: x[1], default=(None, float('inf'))
        )
        if fastest_compression[0]:
            insights['fastest_compression'] = f"{fastest_compression[0]} is fastest at {fastest_compression[1]:.2f}ms"
        
        # Find best balance
        balanced_scores = []
        for k, v in results.items():
            if 'error' not in v:
                ratio = v.get('compression_ratio', 0)
                speed = 1 / max(v.get('compression_time_ms', {}).get('mean', 1), 0.1)
                balanced_scores.append((k, ratio * speed))
        
        if balanced_scores:
            best_balance = max(balanced_scores, key=lambda x: x[1])
            insights['best_balance'] = f"{best_balance[0]} offers best speed/compression balance"
        
        return insights
