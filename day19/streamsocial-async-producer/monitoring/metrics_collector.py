import psutil
import requests
import time
import json
from datetime import datetime

class SystemMetricsCollector:
    def __init__(self, api_url='http://localhost:5000'):
        self.api_url = api_url
        
    def collect_system_metrics(self):
        """Collect system performance metrics"""
        return {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'disk_io': psutil.disk_io_counters()._asdict() if psutil.disk_io_counters() else {},
            'network_io': psutil.net_io_counters()._asdict() if psutil.net_io_counters() else {},
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def collect_app_metrics(self):
        """Collect application metrics from API"""
        try:
            response = requests.get(f'{self.api_url}/metrics', timeout=5)
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            print(f"Failed to collect app metrics: {e}")
        return {}
    
    def run_monitoring(self, duration=60, interval=5):
        """Run monitoring for specified duration"""
        print(f"📈 Starting metrics collection for {duration} seconds...")
        
        start_time = time.time()
        metrics_log = []
        
        while time.time() - start_time < duration:
            timestamp = datetime.utcnow()
            
            system_metrics = self.collect_system_metrics()
            app_metrics = self.collect_app_metrics()
            
            combined_metrics = {
                'timestamp': timestamp.isoformat(),
                'system': system_metrics,
                'application': app_metrics
            }
            
            metrics_log.append(combined_metrics)
            
            # Print current status
            print(f"[{timestamp.strftime('%H:%M:%S')}] "
                  f"CPU: {system_metrics['cpu_percent']:.1f}% | "
                  f"Memory: {system_metrics['memory_percent']:.1f}% | "
                  f"App Messages: {app_metrics.get('sent', 0)}")
            
            time.sleep(interval)
        
        # Save metrics to file
        with open(f'metrics_{int(time.time())}.json', 'w') as f:
            json.dump(metrics_log, f, indent=2)
        
        print(f"✅ Monitoring complete. Metrics saved.")
        return metrics_log

if __name__ == '__main__':
    collector = SystemMetricsCollector()
    collector.run_monitoring(duration=120, interval=2)
