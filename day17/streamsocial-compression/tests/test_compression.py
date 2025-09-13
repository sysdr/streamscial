import pytest
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from compression.analyzer import CompressionAnalyzer
from compression.streamsocial_data import StreamSocialDataGenerator

def test_compression_analyzer_init():
    analyzer = CompressionAnalyzer()
    assert len(analyzer.algorithms) == 4
    assert len(analyzer.decompressors) == 4

def test_all_algorithms_compress_decompress():
    analyzer = CompressionAnalyzer()
    data_generator = StreamSocialDataGenerator()
    test_data = data_generator.generate_data("user_profile", 10)
    
    for algorithm in analyzer.algorithms.keys():
        # Test compression
        compressed = analyzer.compress_data(test_data, algorithm)
        assert len(compressed) > 0
        assert len(compressed) != len(test_data)  # Should be different
        
        # Test decompression
        decompressed = analyzer.decompress_data(compressed, algorithm)
        assert decompressed == test_data  # Should be identical

def test_benchmark_functionality():
    analyzer = CompressionAnalyzer()
    data_generator = StreamSocialDataGenerator()
    test_data = data_generator.generate_data("post_metadata", 5)
    
    result = analyzer.benchmark_algorithm(test_data, "snappy", iterations=5)
    
    assert "algorithm" in result
    assert "compression_ratio" in result
    assert "compression_time_ms" in result
    assert "decompression_time_ms" in result
    assert result["algorithm"] == "snappy"
    assert result["compression_ratio"] > 0

def test_data_generation():
    generator = StreamSocialDataGenerator()
    
    # Test user profile generation
    user_data = generator.generate_data("user_profile", 5)
    assert len(user_data) > 0
    
    # Test post metadata generation
    post_data = generator.generate_data("post_metadata", 3)
    assert len(post_data) > 0
    
    # Test timeline updates
    timeline_data = generator.generate_data("timeline_update", 2)
    assert len(timeline_data) > 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
