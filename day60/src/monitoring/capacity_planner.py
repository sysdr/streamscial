"""
Capacity Planning Calculator
Calculates exact infrastructure requirements for scale
"""
import json
from typing import Dict
import math

class CapacityPlanner:
    def __init__(self):
        self.constants = {
            "seconds_per_day": 86400,
            "peak_traffic_ratio": 0.2,  # 80% traffic in 20% of time
            "safety_margin": 3.0,
            "replication_factor": 3,
            "partition_throughput": 10000,  # msg/sec per partition
            "broker_throughput_mb": 100,    # MB/sec per broker
        }
    
    def calculate_requirements(self, dau: int, events_per_user: int) -> Dict:
        """Calculate infrastructure requirements"""
        
        # Peak requests per second
        total_events_per_day = dau * events_per_user
        avg_rps = total_events_per_day / self.constants["seconds_per_day"]
        peak_rps = avg_rps / self.constants["peak_traffic_ratio"]
        
        # Kafka partitions needed
        min_partitions = math.ceil(peak_rps / self.constants["partition_throughput"])
        recommended_partitions = min_partitions * self.constants["replication_factor"]
        
        # Kafka brokers needed
        avg_msg_size_kb = 2  # 2KB per message
        peak_throughput_mb = (peak_rps * avg_msg_size_kb) / 1024
        
        # Multiply by consumers (fanout)
        consumer_groups = 8
        total_egress_mb = peak_throughput_mb * consumer_groups
        
        min_brokers = math.ceil(
            (peak_throughput_mb * self.constants["replication_factor"]) /
            self.constants["broker_throughput_mb"]
        )
        recommended_brokers = max(6, min_brokers)
        
        # Storage requirements
        retention_days = 7
        compressed_ratio = 0.3  # 70% compression
        daily_storage_gb = (total_events_per_day * avg_msg_size_kb / 1024 / 1024) * compressed_ratio
        total_storage_tb = (daily_storage_gb * retention_days) / 1024
        
        # Memory requirements
        offset_metadata_mb = (min_partitions * consumer_groups * 0.1)  # 100KB per partition-consumer
        broker_metadata_mb = 2000  # Base overhead per broker
        total_memory_gb = ((broker_metadata_mb * recommended_brokers) + offset_metadata_mb) / 1024
        
        # Network requirements
        network_gbps = (total_egress_mb * 8) / 1000  # Convert MB/s to Gbps
        
        # Cost estimation (AWS pricing)
        broker_instance_cost = 300  # r5.2xlarge per month
        storage_cost_per_tb = 100   # EBS gp3
        network_cost_per_tb = 90    # Data transfer
        
        monthly_cost = (
            (recommended_brokers * broker_instance_cost) +
            (total_storage_tb * storage_cost_per_tb) +
            (network_gbps * 30 * 24 * 3600 / 8 / 1024 * network_cost_per_tb)  # Approximate
        )
        
        return {
            "input_parameters": {
                "daily_active_users": dau,
                "events_per_user": events_per_user,
                "total_events_per_day": total_events_per_day
            },
            "throughput": {
                "average_rps": round(avg_rps, 2),
                "peak_rps": round(peak_rps, 2),
                "peak_throughput_mb_per_sec": round(peak_throughput_mb, 2),
                "total_egress_mb_per_sec": round(total_egress_mb, 2)
            },
            "kafka_infrastructure": {
                "minimum_partitions": min_partitions,
                "recommended_partitions": recommended_partitions,
                "minimum_brokers": min_brokers,
                "recommended_brokers": recommended_brokers,
                "replication_factor": self.constants["replication_factor"]
            },
            "storage": {
                "daily_growth_gb": round(daily_storage_gb, 2),
                "retention_days": retention_days,
                "total_storage_tb": round(total_storage_tb, 2)
            },
            "network": {
                "required_bandwidth_gbps": round(network_gbps, 2),
                "recommended_links": "2x 100 Gbps per datacenter"
            },
            "memory": {
                "total_ram_gb": round(total_memory_gb, 2),
                "ram_per_broker_gb": round(total_memory_gb / recommended_brokers, 2)
            },
            "estimated_monthly_cost_usd": round(monthly_cost, 2),
            "cost_per_million_events": round(monthly_cost / (total_events_per_day * 30 / 1000000), 4)
        }

if __name__ == "__main__":
    planner = CapacityPlanner()
    
    # Calculate for 100M DAU
    result = planner.calculate_requirements(
        dau=100_000_000,
        events_per_user=10
    )
    
    print("Capacity Planning for 100M Daily Active Users")
    print("=" * 60)
    print(json.dumps(result, indent=2))
