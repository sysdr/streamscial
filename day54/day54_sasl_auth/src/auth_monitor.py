"""Real-time authentication monitoring system."""

import time
import json
from typing import Dict, List
from datetime import datetime, timedelta
from collections import defaultdict
from threading import Thread, Lock


class AuthenticationMonitor:
    """Monitor and track authentication events."""
    
    def __init__(self):
        self.stats = {
            'total_attempts': 0,
            'successful_auths': 0,
            'failed_auths': 0,
            'scram_auths': 0,
            'plain_auths': 0,
            'auth_by_user': defaultdict(lambda: {'success': 0, 'failure': 0}),
            'recent_events': [],
            'latency_samples': []
        }
        self.lock = Lock()
        
    def record_auth_attempt(self, username: str, mechanism: str, 
                           success: bool, latency_ms: float = 0):
        """Record an authentication attempt."""
        with self.lock:
            self.stats['total_attempts'] += 1
            
            if success:
                self.stats['successful_auths'] += 1
                self.stats['auth_by_user'][username]['success'] += 1
            else:
                self.stats['failed_auths'] += 1
                self.stats['auth_by_user'][username]['failure'] += 1
            
            if mechanism == 'SCRAM-SHA-512':
                self.stats['scram_auths'] += 1
            elif mechanism == 'PLAIN':
                self.stats['plain_auths'] += 1
            
            if latency_ms > 0:
                self.stats['latency_samples'].append(latency_ms)
                # Keep only last 1000 samples
                if len(self.stats['latency_samples']) > 1000:
                    self.stats['latency_samples'] = self.stats['latency_samples'][-1000:]
            
            event = {
                'timestamp': datetime.now().isoformat(),
                'username': username,
                'mechanism': mechanism,
                'success': success,
                'latency_ms': latency_ms
            }
            
            self.stats['recent_events'].append(event)
            # Keep only last 100 events
            if len(self.stats['recent_events']) > 100:
                self.stats['recent_events'] = self.stats['recent_events'][-100:]
    
    def get_stats(self) -> Dict:
        """Get current statistics."""
        with self.lock:
            avg_latency = (
                sum(self.stats['latency_samples']) / len(self.stats['latency_samples'])
                if self.stats['latency_samples'] else 0
            )
            
            success_rate = (
                (self.stats['successful_auths'] / self.stats['total_attempts'] * 100)
                if self.stats['total_attempts'] > 0 else 0
            )
            
            return {
                'total_attempts': self.stats['total_attempts'],
                'successful_auths': self.stats['successful_auths'],
                'failed_auths': self.stats['failed_auths'],
                'success_rate': round(success_rate, 2),
                'scram_auths': self.stats['scram_auths'],
                'plain_auths': self.stats['plain_auths'],
                'avg_latency_ms': round(avg_latency, 2),
                'auth_by_user': dict(self.stats['auth_by_user']),
                'recent_events': self.stats['recent_events'][-20:]
            }
    
    def get_failure_rate_by_user(self) -> List[Dict]:
        """Get users with high failure rates."""
        with self.lock:
            failure_rates = []
            
            for username, counts in self.stats['auth_by_user'].items():
                total = counts['success'] + counts['failure']
                if total > 0:
                    failure_rate = (counts['failure'] / total) * 100
                    if failure_rate > 10:  # Flag users with >10% failure rate
                        failure_rates.append({
                            'username': username,
                            'failure_rate': round(failure_rate, 2),
                            'total_attempts': total,
                            'failures': counts['failure']
                        })
            
            return sorted(failure_rates, key=lambda x: x['failure_rate'], reverse=True)
