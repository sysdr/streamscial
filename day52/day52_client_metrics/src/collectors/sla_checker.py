import json
import time
from collections import deque

class SLAChecker:
    def __init__(self, sla_config_path):
        with open(sla_config_path, 'r') as f:
            config = json.load(f)
            self.sla_configs = config['consumer_groups']
        
        self.violation_history = {}
        self.lag_history = {}
        
    def check_compliance(self, group_id, lag_data, throughput, latency):
        """Check if consumer group meets SLA requirements"""
        if group_id not in self.sla_configs:
            return {'status': 'unknown', 'health_score': 0}
        
        sla = self.sla_configs[group_id]
        violations = []
        
        # Check lag
        lag_ratio = lag_data / sla['max_lag_messages'] if sla['max_lag_messages'] > 0 else 0
        if lag_data > sla['max_lag_messages']:
            violations.append(f"Lag {lag_data} exceeds max {sla['max_lag_messages']}")
        
        # Check throughput
        throughput_ratio = throughput / sla['min_throughput_rps'] if sla['min_throughput_rps'] > 0 else 1
        if throughput < sla['min_throughput_rps']:
            violations.append(f"Throughput {throughput:.0f} below min {sla['min_throughput_rps']}")
        
        # Check latency
        latency_ratio = latency / sla['max_e2e_latency_ms'] if sla['max_e2e_latency_ms'] > 0 else 0
        if latency > sla['max_e2e_latency_ms']:
            violations.append(f"Latency {latency:.1f}ms exceeds max {sla['max_e2e_latency_ms']}ms")
        
        # Calculate health score (0-100)
        lag_penalty = min(40, lag_ratio * 40) if lag_data > sla['max_lag_messages'] else lag_ratio * 20
        throughput_penalty = min(30, max(0, (1 - throughput_ratio) * 30)) if throughput < sla['min_throughput_rps'] else 0
        latency_penalty = min(30, latency_ratio * 30) if latency > sla['max_e2e_latency_ms'] else latency_ratio * 10
        
        health_score = max(0, 100 - lag_penalty - throughput_penalty - latency_penalty)
        
        # Determine status
        if health_score >= 90:
            status = 'healthy'
        elif health_score >= 70:
            status = 'warning'
        else:
            status = 'critical'
        
        # Track lag velocity
        if group_id not in self.lag_history:
            self.lag_history[group_id] = deque(maxlen=60)  # 5 minutes at 5-second intervals
        
        self.lag_history[group_id].append({'timestamp': time.time(), 'lag': lag_data})
        
        lag_velocity = 0
        if len(self.lag_history[group_id]) >= 2:
            recent = list(self.lag_history[group_id])
            time_diff = recent[-1]['timestamp'] - recent[0]['timestamp']
            lag_diff = recent[-1]['lag'] - recent[0]['lag']
            if time_diff > 0:
                lag_velocity = lag_diff / (time_diff / 60)  # messages per minute
        
        # Predict time to SLA breach
        time_to_breach = None
        if lag_velocity > 0 and lag_data < sla['max_lag_messages']:
            remaining_lag = sla['max_lag_messages'] - lag_data
            time_to_breach = remaining_lag / lag_velocity  # minutes
        
        return {
            'status': status,
            'health_score': round(health_score, 1),
            'violations': violations,
            'lag_velocity': round(lag_velocity, 0),
            'time_to_breach_min': round(time_to_breach, 1) if time_to_breach else None,
            'sla': sla
        }
