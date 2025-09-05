import hashlib
from typing import Dict, List, Any
from collections import defaultdict, Counter
import structlog

logger = structlog.get_logger(__name__)

class PartitionAnalyzer:
    def __init__(self, partition_count: int = 6):
        self.partition_count = partition_count
        self.key_distribution = defaultdict(list)
        self.partition_loads = defaultdict(int)
    
    def calculate_partition_for_key(self, key: str) -> int:
        """Calculate which partition a key maps to using same hash as Kafka"""
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_value % self.partition_count
    
    def analyze_key_distribution(self, keys: List[str]) -> Dict[str, Any]:
        """Analyze how keys distribute across partitions"""
        partition_assignment = {}
        partition_counts = Counter()
        
        for key in keys:
            partition = self.calculate_partition_for_key(key)
            partition_assignment[key] = partition
            partition_counts[partition] += 1
            self.key_distribution[partition].append(key)
            self.partition_loads[partition] += 1
        
        # Calculate distribution statistics
        total_keys = len(keys)
        avg_per_partition = total_keys / self.partition_count
        
        distribution_stats = {
            'total_keys': total_keys,
            'partition_count': self.partition_count,
            'average_per_partition': avg_per_partition,
            'partition_assignment': partition_assignment,
            'partition_counts': dict(partition_counts),
            'load_balance_score': self._calculate_load_balance_score(partition_counts),
            'hottest_partition': partition_counts.most_common(1)[0] if partition_counts else None,
            'coldest_partition': partition_counts.most_common()[-1] if partition_counts else None
        }
        
        return distribution_stats
    
    def _calculate_load_balance_score(self, partition_counts: Counter) -> float:
        """Calculate load balance score (0-1, where 1 is perfectly balanced)"""
        if not partition_counts:
            return 0.0
        
        counts = list(partition_counts.values())
        if len(counts) == 0:
            return 0.0
        
        mean_load = sum(counts) / len(counts)
        variance = sum((x - mean_load) ** 2 for x in counts) / len(counts)
        
        # Normalize score (lower variance = higher score)
        max_possible_variance = mean_load ** 2
        if max_possible_variance == 0:
            return 1.0
        
        balance_score = 1 - (variance / max_possible_variance)
        return max(0.0, min(1.0, balance_score))
    
    def simulate_user_activity(self, user_ids: List[str], posts_per_user: List[int]) -> Dict[str, Any]:
        """Simulate user posting activity and analyze partition distribution"""
        all_keys = []
        user_partition_mapping = {}
        
        for user_id, post_count in zip(user_ids, posts_per_user):
            partition_key = f"user:{user_id}"
            partition = self.calculate_partition_for_key(partition_key)
            user_partition_mapping[user_id] = {
                'partition': partition,
                'post_count': post_count,
                'partition_key': partition_key
            }
            
            # Add keys for each post
            all_keys.extend([partition_key] * post_count)
        
        distribution_analysis = self.analyze_key_distribution(all_keys)
        
        return {
            'user_mapping': user_partition_mapping,
            'distribution_analysis': distribution_analysis,
            'partition_workload': self._calculate_partition_workload(user_partition_mapping)
        }
    
    def _calculate_partition_workload(self, user_mapping: Dict) -> Dict[int, Dict]:
        """Calculate workload per partition"""
        partition_workload = defaultdict(lambda: {'users': 0, 'total_posts': 0, 'user_list': []})
        
        for user_id, info in user_mapping.items():
            partition = info['partition']
            partition_workload[partition]['users'] += 1
            partition_workload[partition]['total_posts'] += info['post_count']
            partition_workload[partition]['user_list'].append(user_id)
        
        return dict(partition_workload)
    
    def suggest_optimization(self, distribution_stats: Dict) -> List[str]:
        """Suggest optimizations based on distribution analysis"""
        suggestions = []
        
        load_balance_score = distribution_stats['load_balance_score']
        
        if load_balance_score < 0.7:
            suggestions.append("Consider using composite keys (user_id + content_type) for better distribution")
        
        if distribution_stats['hottest_partition'][1] > distribution_stats['average_per_partition'] * 2:
            suggestions.append("Hot partition detected - consider splitting high-activity users")
        
        if len(distribution_stats['partition_counts']) < self.partition_count * 0.8:
            suggestions.append("Some partitions are unused - verify key variety")
        
        return suggestions
