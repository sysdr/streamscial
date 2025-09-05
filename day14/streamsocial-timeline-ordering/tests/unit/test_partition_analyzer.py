import pytest
from src.utils.partition_analyzer import PartitionAnalyzer

class TestPartitionAnalyzer:
    def test_partition_calculation_consistency(self):
        analyzer = PartitionAnalyzer(partition_count=6)
        
        key = "user:test123"
        partition1 = analyzer.calculate_partition_for_key(key)
        partition2 = analyzer.calculate_partition_for_key(key)
        
        assert partition1 == partition2
        assert 0 <= partition1 < 6
    
    def test_key_distribution_analysis(self):
        analyzer = PartitionAnalyzer(partition_count=3)
        
        keys = ["user:alice", "user:bob", "user:charlie", "user:alice", "user:bob"]
        analysis = analyzer.analyze_key_distribution(keys)
        
        assert analysis['total_keys'] == 5
        assert analysis['partition_count'] == 3
        assert 'partition_assignment' in analysis
        assert 'load_balance_score' in analysis
        assert 0 <= analysis['load_balance_score'] <= 1
    
    def test_user_activity_simulation(self):
        analyzer = PartitionAnalyzer(partition_count=3)
        
        user_ids = ["alice", "bob", "charlie"]
        posts_per_user = [5, 10, 3]
        
        simulation = analyzer.simulate_user_activity(user_ids, posts_per_user)
        
        assert 'user_mapping' in simulation
        assert 'distribution_analysis' in simulation
        assert 'partition_workload' in simulation
        
        # Check user mapping
        assert len(simulation['user_mapping']) == 3
        for user_id in user_ids:
            assert user_id in simulation['user_mapping']
            assert 'partition' in simulation['user_mapping'][user_id]
            assert 'post_count' in simulation['user_mapping'][user_id]
    
    def test_load_balance_score(self):
        analyzer = PartitionAnalyzer(partition_count=2)
        
        # Perfectly balanced
        balanced_keys = ["user:a", "user:b", "user:c", "user:d"]  # Assume even distribution
        analysis1 = analyzer.analyze_key_distribution(balanced_keys)
        
        # Very unbalanced
        unbalanced_keys = ["user:same"] * 10  # All go to same partition
        analysis2 = analyzer.analyze_key_distribution(unbalanced_keys)
        
        # Balanced should have higher score
        assert analysis1['load_balance_score'] >= analysis2['load_balance_score']
