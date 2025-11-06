"""
Kafka Connect Cluster Monitor
Tracks worker health, task distribution, and rebalance events
"""
import time
import requests
import json
from datetime import datetime
from typing import Dict, List, Any
import threading
import os


class ConnectClusterMonitor:
    def __init__(self, worker_urls: List[str]):
        self.worker_urls = worker_urls
        self.metrics = {
            'workers': {},
            'connectors': {},
            'tasks': {},
            'rebalance_events': [],
            'last_update': None
        }
        self.running = False
        self.use_demo_data = os.environ.get('USE_DEMO_DATA', 'false').lower() == 'true'
        self.no_connectors_count = 0
        
    def get_worker_status(self, worker_url: str) -> Dict[str, Any]:
        """Get status of a single worker"""
        try:
            response = requests.get(f"{worker_url}/", timeout=5)
            if response.status_code == 200:
                return {
                    'url': worker_url,
                    'status': 'healthy',
                    'version': response.json().get('version', 'unknown'),
                    'commit': response.json().get('commit', 'unknown')
                }
        except requests.exceptions.RequestException:
            pass
        
        return {
            'url': worker_url,
            'status': 'unhealthy',
            'version': None,
            'commit': None
        }
    
    def get_connectors(self, worker_url: str) -> List[str]:
        """Get list of connectors from any worker"""
        try:
            response = requests.get(f"{worker_url}/connectors", timeout=5)
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException:
            pass
        return []
    
    def get_connector_status(self, worker_url: str, connector_name: str) -> Dict[str, Any]:
        """Get detailed connector status"""
        try:
            response = requests.get(
                f"{worker_url}/connectors/{connector_name}/status",
                timeout=5
            )
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException:
            pass
        return {}
    
    def get_connector_tasks(self, worker_url: str, connector_name: str) -> List[Dict]:
        """Get task assignments for a connector"""
        try:
            response = requests.get(
                f"{worker_url}/connectors/{connector_name}/tasks",
                timeout=5
            )
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException:
            pass
        return []
    
    def detect_rebalance(self, new_task_distribution: Dict) -> bool:
        """Detect if a rebalance occurred"""
        if not self.metrics['tasks']:
            return False
        
        old_distribution = {
            task_id: task['worker_id']
            for task_id, task in self.metrics['tasks'].items()
        }
        
        changed = old_distribution != new_task_distribution
        if changed:
            self.metrics['rebalance_events'].append({
                'timestamp': datetime.now().isoformat(),
                'old_distribution': old_distribution,
                'new_distribution': new_task_distribution
            })
        
        return changed
    
    def collect_metrics(self) -> Dict[str, Any]:
        """Collect all cluster metrics"""
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'workers': {},
            'connectors': {},
            'tasks': {},
            'cluster_health': 'healthy'
        }
        
        # Check all workers
        healthy_workers = []
        for worker_url in self.worker_urls:
            worker_status = self.get_worker_status(worker_url)
            worker_id = worker_url.split(':')[-1]
            metrics['workers'][worker_id] = worker_status
            
            if worker_status['status'] == 'healthy':
                healthy_workers.append(worker_url)
        
        # If we have at least one healthy worker, get connector info
        if healthy_workers:
            primary_worker = healthy_workers[0]
            connectors = self.get_connectors(primary_worker)
            
            task_distribution = {}
            for connector_name in connectors:
                status = self.get_connector_status(primary_worker, connector_name)
                tasks = self.get_connector_tasks(primary_worker, connector_name)
                
                metrics['connectors'][connector_name] = {
                    'name': status.get('name', connector_name),
                    'state': status.get('connector', {}).get('state', 'UNKNOWN'),
                    'worker_id': status.get('connector', {}).get('worker_id', 'unknown'),
                    'tasks': []
                }
                
                for task in status.get('tasks', []):
                    task_id = f"{connector_name}-{task['id']}"
                    task_info = {
                        'id': task['id'],
                        'state': task['state'],
                        'worker_id': task['worker_id'],
                        'trace': task.get('trace', '')
                    }
                    metrics['connectors'][connector_name]['tasks'].append(task_info)
                    metrics['tasks'][task_id] = task_info
                    task_distribution[task_id] = task['worker_id']
            
            # Detect rebalances
            self.detect_rebalance(task_distribution)
        
        # Determine overall cluster health
        healthy_count = sum(1 for w in metrics['workers'].values() if w['status'] == 'healthy')
        if healthy_count == 0:
            metrics['cluster_health'] = 'critical'
        elif healthy_count < len(self.worker_urls):
            metrics['cluster_health'] = 'degraded'
        
        self.metrics = metrics
        self.metrics['rebalance_events'] = self.metrics.get('rebalance_events', [])[-10:]  # Keep last 10
        
        return metrics
    
    def start_monitoring(self, interval: int = 2):
        """Start continuous monitoring"""
        self.running = True
        
        def monitor_loop():
            while self.running:
                try:
                    self.collect_metrics()
                    time.sleep(interval)
                except Exception as e:
                    print(f"Monitoring error: {e}")
                    time.sleep(interval)
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.running = False
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot"""
        # Check if demo mode is forced
        if self.use_demo_data or os.environ.get('FORCE_DEMO', 'false').lower() == 'true':
            from .demo_data import get_demo_metrics
            return get_demo_metrics()
        
        metrics = {
            **self.metrics,
            'rebalance_count': len(self.metrics.get('rebalance_events', []))
        }
        
        # If no connectors found, use demo data as fallback
        if not metrics.get('connectors'):
            self.no_connectors_count += 1
            if self.no_connectors_count > 2:
                print("No connectors found, switching to demo data...")
                from .demo_data import get_demo_metrics
                return get_demo_metrics()
        else:
            self.no_connectors_count = 0
            
        return metrics
