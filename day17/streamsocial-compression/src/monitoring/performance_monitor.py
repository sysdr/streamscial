import psutil
import time
import threading
from typing import Dict, List, Any
from collections import deque
import json

class PerformanceMonitor:
    def __init__(self, history_size: int = 100):
        self.history_size = history_size
        self.cpu_history = deque(maxlen=history_size)
        self.memory_history = deque(maxlen=history_size)
        self.network_history = deque(maxlen=history_size)
        self.disk_history = deque(maxlen=history_size)
        
        self.monitoring = False
        self.monitor_thread = None
        
        # Initial network stats
        self.last_network_stats = psutil.net_io_counters()
        self.last_network_time = time.time()
    
    def start_monitoring(self):
        if not self.monitoring:
            self.monitoring = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
    
    def stop_monitoring(self):
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
    
    def _monitor_loop(self):
        while self.monitoring:
            self._collect_metrics()
            time.sleep(1)
    
    def _collect_metrics(self):
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=None)
        cpu_count = psutil.cpu_count()
        load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else [0, 0, 0]
        
        # Memory metrics
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        # Network metrics
        current_network = psutil.net_io_counters()
        current_time = time.time()
        
        time_delta = current_time - self.last_network_time
        bytes_sent_per_sec = (current_network.bytes_sent - self.last_network_stats.bytes_sent) / time_delta
        bytes_recv_per_sec = (current_network.bytes_recv - self.last_network_stats.bytes_recv) / time_delta
        
        self.last_network_stats = current_network
        self.last_network_time = current_time
        
        # Disk metrics
        disk = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()
        
        timestamp = time.time()
        
        self.cpu_history.append({
            'timestamp': timestamp,
            'cpu_percent': cpu_percent,
            'cpu_count': cpu_count,
            'load_avg_1m': load_avg[0],
            'load_avg_5m': load_avg[1],
            'load_avg_15m': load_avg[2]
        })
        
        self.memory_history.append({
            'timestamp': timestamp,
            'total': memory.total,
            'available': memory.available,
            'used': memory.used,
            'percent': memory.percent,
            'swap_total': swap.total,
            'swap_used': swap.used,
            'swap_percent': swap.percent
        })
        
        self.network_history.append({
            'timestamp': timestamp,
            'bytes_sent_per_sec': bytes_sent_per_sec,
            'bytes_recv_per_sec': bytes_recv_per_sec,
            'total_bytes_sent': current_network.bytes_sent,
            'total_bytes_recv': current_network.bytes_recv,
            'packets_sent': current_network.packets_sent,
            'packets_recv': current_network.packets_recv
        })
        
        self.disk_history.append({
            'timestamp': timestamp,
            'total': disk.total,
            'used': disk.used,
            'free': disk.free,
            'percent': disk.percent,
            'read_bytes': disk_io.read_bytes if disk_io else 0,
            'write_bytes': disk_io.write_bytes if disk_io else 0
        })
    
    def get_current_metrics(self) -> Dict[str, Any]:
        # Force immediate collection
        self._collect_metrics()
        
        return {
            'cpu': list(self.cpu_history)[-1] if self.cpu_history else {},
            'memory': list(self.memory_history)[-1] if self.memory_history else {},
            'network': list(self.network_history)[-1] if self.network_history else {},
            'disk': list(self.disk_history)[-1] if self.disk_history else {},
            'timestamp': time.time()
        }
    
    def get_historical_metrics(self, minutes: int = 5) -> Dict[str, List[Dict]]:
        cutoff_time = time.time() - (minutes * 60)
        
        return {
            'cpu': [m for m in self.cpu_history if m['timestamp'] > cutoff_time],
            'memory': [m for m in self.memory_history if m['timestamp'] > cutoff_time],
            'network': [m for m in self.network_history if m['timestamp'] > cutoff_time],
            'disk': [m for m in self.disk_history if m['timestamp'] > cutoff_time]
        }
