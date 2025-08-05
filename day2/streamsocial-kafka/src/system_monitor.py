"""
StreamSocial System Monitor - Real Host System Events
Captures actual system metrics and events for monitoring dashboard
"""

import psutil
import time
import json
import threading
import random
from datetime import datetime
import os
import platform

class SystemEventMonitor:
    def __init__(self):
        self.system_info = self._get_system_info()
        self.events = []
        self.max_events = 100
        self.running = True
        
    def _get_system_info(self):
        """Get basic system information"""
        return {
            'hostname': platform.node(),
            'platform': platform.system(),
            'platform_version': platform.version(),
            'architecture': platform.machine(),
            'processor': platform.processor(),
            'python_version': platform.python_version(),
            'cpu_count': psutil.cpu_count(),
            'memory_total': psutil.virtual_memory().total
        }
    
    def capture_cpu_event(self):
        """Capture CPU usage event"""
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_freq = psutil.cpu_freq()
        
        return {
            'event_type': 'cpu_usage',
            'timestamp': int(time.time() * 1000),
            'cpu_percent': cpu_percent,
            'cpu_freq_current': cpu_freq.current if cpu_freq else None,
            'cpu_freq_max': cpu_freq.max if cpu_freq else None,
            'cpu_count': psutil.cpu_count(),
            'cpu_count_logical': psutil.cpu_count(logical=True)
        }
    
    def capture_memory_event(self):
        """Capture memory usage event"""
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        return {
            'event_type': 'memory_usage',
            'timestamp': int(time.time() * 1000),
            'memory_percent': memory.percent,
            'memory_used': memory.used,
            'memory_available': memory.available,
            'memory_total': memory.total,
            'swap_percent': swap.percent,
            'swap_used': swap.used,
            'swap_total': swap.total
        }
    
    def capture_disk_event(self):
        """Capture disk usage event"""
        disk_usage = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()
        
        return {
            'event_type': 'disk_usage',
            'timestamp': int(time.time() * 1000),
            'disk_percent': disk_usage.percent,
            'disk_used': disk_usage.used,
            'disk_free': disk_usage.free,
            'disk_total': disk_usage.total,
            'disk_read_bytes': disk_io.read_bytes if disk_io else 0,
            'disk_write_bytes': disk_io.write_bytes if disk_io else 0,
            'disk_read_count': disk_io.read_count if disk_io else 0,
            'disk_write_count': disk_io.write_count if disk_io else 0
        }
    
    def capture_network_event(self):
        """Capture network activity event"""
        network_io = psutil.net_io_counters()
        network_connections = len(psutil.net_connections())
        
        return {
            'event_type': 'network_activity',
            'timestamp': int(time.time() * 1000),
            'bytes_sent': network_io.bytes_sent,
            'bytes_recv': network_io.bytes_recv,
            'packets_sent': network_io.packets_sent,
            'packets_recv': network_io.packets_recv,
            'connections_count': network_connections
        }
    
    def capture_process_event(self):
        """Capture process information event"""
        processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
            try:
                processes.append({
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'cpu_percent': proc.info['cpu_percent'],
                    'memory_percent': proc.info['memory_percent']
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Get top 5 processes by CPU usage
        top_cpu = sorted(processes, key=lambda x: x['cpu_percent'], reverse=True)[:5]
        top_memory = sorted(processes, key=lambda x: x['memory_percent'], reverse=True)[:5]
        
        return {
            'event_type': 'process_info',
            'timestamp': int(time.time() * 1000),
            'total_processes': len(processes),
            'top_cpu_processes': top_cpu,
            'top_memory_processes': top_memory
        }
    
    def capture_system_load_event(self):
        """Capture system load average (Unix-like systems)"""
        try:
            load_avg = os.getloadavg()
            return {
                'event_type': 'system_load',
                'timestamp': int(time.time() * 1000),
                'load_1min': load_avg[0],
                'load_5min': load_avg[1],
                'load_15min': load_avg[2]
            }
        except AttributeError:
            # Windows doesn't have load average
            return {
                'event_type': 'system_load',
                'timestamp': int(time.time() * 1000),
                'load_1min': None,
                'load_5min': None,
                'load_15min': None,
                'note': 'Load average not available on this system'
            }
    
    def capture_battery_event(self):
        """Capture battery information (if available)"""
        try:
            battery = psutil.sensors_battery()
            if battery:
                return {
                    'event_type': 'battery_status',
                    'timestamp': int(time.time() * 1000),
                    'percent': battery.percent,
                    'power_plugged': battery.power_plugged,
                    'time_left': battery.secsleft if battery.secsleft != -1 else None
                }
        except:
            pass
        return None
    
    def capture_temperature_event(self):
        """Capture temperature sensors (if available)"""
        try:
            temps = psutil.sensors_temperatures()
            if temps:
                temp_data = {}
                for name, entries in temps.items():
                    temp_data[name] = [{
                        'current': entry.current,
                        'high': entry.high,
                        'critical': entry.critical
                    } for entry in entries]
                
                return {
                    'event_type': 'temperature',
                    'timestamp': int(time.time() * 1000),
                    'sensors': temp_data
                }
        except:
            pass
        return None
    
    def generate_system_event(self):
        """Generate a random system event based on current metrics"""
        event_types = [
            self.capture_cpu_event,
            self.capture_memory_event,
            self.capture_disk_event,
            self.capture_network_event,
            self.capture_process_event,
            self.capture_system_load_event
        ]
        
        # Add optional events if available
        battery_event = self.capture_battery_event()
        if battery_event:
            event_types.append(lambda: battery_event)
        
        temp_event = self.capture_temperature_event()
        if temp_event:
            event_types.append(lambda: temp_event)
        
        # Select random event type
        event_generator = random.choice(event_types)
        event = event_generator()
        
        # Add system info to event
        event['system_info'] = self.system_info
        event['hostname'] = self.system_info['hostname']
        
        return event
    
    def start_monitoring(self):
        """Start continuous system monitoring"""
        def monitor_loop():
            while self.running:
                try:
                    # Generate system event
                    event = self.generate_system_event()
                    
                    # Add to events list
                    self.events.append(event)
                    
                    # Keep only recent events
                    if len(self.events) > self.max_events:
                        self.events.pop(0)
                    
                    # Sleep for a random interval (1-3 seconds)
                    time.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    print(f"Error capturing system event: {e}")
                    time.sleep(5)
        
        # Start monitoring in background thread
        threading.Thread(target=monitor_loop, daemon=True).start()
        print("🖥️ System monitoring started")
    
    def get_recent_events(self, count=10):
        """Get recent system events"""
        return self.events[-count:] if self.events else []
    
    def get_system_summary(self):
        """Get current system summary"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_gb': round(memory.used / (1024**3), 2),
                'memory_total_gb': round(memory.total / (1024**3), 2),
                'disk_percent': disk.percent,
                'disk_used_gb': round(disk.used / (1024**3), 2),
                'disk_total_gb': round(disk.total / (1024**3), 2),
                'process_count': len(psutil.pids()),
                'timestamp': int(time.time() * 1000)
            }
        except Exception as e:
            return {'error': str(e)}
    
    def stop_monitoring(self):
        """Stop system monitoring"""
        self.running = False
        print("🛑 System monitoring stopped")

# Global instance
system_monitor = SystemEventMonitor()

if __name__ == "__main__":
    # Test the system monitor
    print("🧪 Testing System Monitor...")
    print(f"System: {system_monitor.system_info['platform']} {system_monitor.system_info['platform_version']}")
    print(f"Hostname: {system_monitor.system_info['hostname']}")
    print(f"CPU Cores: {system_monitor.system_info['cpu_count']}")
    print(f"Memory: {round(system_monitor.system_info['memory_total'] / (1024**3), 2)} GB")
    
    # Start monitoring
    system_monitor.start_monitoring()
    
    # Show some events
    time.sleep(5)
    print("\n📊 Recent System Events:")
    for event in system_monitor.get_recent_events(5):
        print(f"  {event['event_type']}: {event.get('cpu_percent', event.get('memory_percent', 'N/A'))}")
    
    # Show system summary
    print("\n📈 System Summary:")
    summary = system_monitor.get_system_summary()
    for key, value in summary.items():
        if key != 'timestamp':
            print(f"  {key}: {value}")
    
    print("\n✅ System monitor test completed") 