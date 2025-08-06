#!/usr/bin/env python3
"""Script to update metrics file for dashboard demo"""

import json
import time
import random
import os
from datetime import datetime

def update_metrics():
    """Update the metrics file with simulated real-time data"""
    
    # Generate realistic partition metrics
    user_actions_partitions = []
    content_interactions_partitions = []
    
    # Generate user-actions partitions (1000 partitions)
    for i in range(50):  # Show first 50 partitions
        throughput = 50 + random.randint(0, 200) + random.random() * 50
        lag = random.randint(0, 10)
        user_actions_partitions.append({
            'partition_id': i,
            'topic_name': 'user-actions',
            'throughput_rps': round(throughput, 1),
            'lag': lag
        })
    
    # Generate content-interactions partitions (500 partitions)
    for i in range(30):  # Show first 30 partitions
        throughput = 75 + random.randint(0, 150) + random.random() * 40
        lag = random.randint(0, 8)
        content_interactions_partitions.append({
            'partition_id': i,
            'topic_name': 'content-interactions',
            'throughput_rps': round(throughput, 1),
            'lag': lag
        })
    
    # Combine all metrics
    all_metrics = user_actions_partitions + content_interactions_partitions
    
    # Calculate cluster health
    total_throughput = sum(m['throughput_rps'] for m in all_metrics)
    avg_throughput = total_throughput / len(all_metrics) if all_metrics else 0
    
    # Generate some hotspots and cold spots
    hotspots = random.sample(range(1000), 5)
    cold_spots = random.sample(range(1000), 3)
    
    metrics_data = {
        'cluster_health': {
            'total_partitions': 1500,
            'active_partitions': 1200,
            'avg_throughput': round(avg_throughput, 1),
            'overall_health': 'healthy',
            'hotspots': hotspots,
            'cold_spots': cold_spots,
            'last_updated': datetime.now().isoformat()
        },
        'partition_metrics': all_metrics
    }
    
    # Write to file
    os.makedirs('monitoring', exist_ok=True)
    with open('monitoring/latest_metrics.json', 'w') as f:
        json.dump(metrics_data, f, indent=2)
    
    print(f"✅ Updated metrics at {datetime.now().strftime('%H:%M:%S')}")

if __name__ == "__main__":
    print("🔄 Starting metrics updater...")
    print("📊 Will update metrics every 30 seconds")
    print("🛑 Press Ctrl+C to stop")
    
    try:
        while True:
            update_metrics()
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n🛑 Metrics updater stopped") 