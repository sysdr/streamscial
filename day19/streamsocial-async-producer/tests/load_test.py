import asyncio
import aiohttp
import time
import json
from concurrent.futures import ThreadPoolExecutor
import threading

class LoadTester:
    def __init__(self, base_url='http://localhost:5000'):
        self.base_url = base_url
        self.results = {
            'sent': 0,
            'success': 0,
            'failed': 0,
            'latencies': []
        }
        self.lock = threading.Lock()
    
    def send_post(self, user_id, content):
        """Send single post"""
        start_time = time.time()
        
        try:
            import requests
            response = requests.post(
                f'{self.base_url}/post',
                json={'user_id': user_id, 'content': content},
                timeout=10
            )
            
            latency = time.time() - start_time
            
            with self.lock:
                self.results['sent'] += 1
                self.results['latencies'].append(latency)
                
                if response.status_code == 200:
                    self.results['success'] += 1
                else:
                    self.results['failed'] += 1
                    print(f"Failed: {response.status_code} - {response.text}")
                    
        except Exception as e:
            with self.lock:
                self.results['failed'] += 1
            print(f"Error: {e}")
    
    def run_load_test(self, num_requests=1000, num_threads=10):
        """Run load test with multiple threads"""
        print(f"🔥 Starting load test: {num_requests} requests with {num_threads} threads")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            
            for i in range(num_requests):
                user_id = f"load_user_{i % 100}"
                content = f"Load test message #{i} - {time.time()}"
                
                future = executor.submit(self.send_post, user_id, content)
                futures.append(future)
            
            # Wait for all requests to complete
            for future in futures:
                future.result()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Calculate statistics
        avg_latency = sum(self.results['latencies']) / len(self.results['latencies']) if self.results['latencies'] else 0
        throughput = self.results['sent'] / duration
        success_rate = (self.results['success'] / self.results['sent']) * 100 if self.results['sent'] > 0 else 0
        
        print("\n📊 Load Test Results:")
        print(f"  Duration: {duration:.2f} seconds")
        print(f"  Total Requests: {self.results['sent']}")
        print(f"  Successful: {self.results['success']}")
        print(f"  Failed: {self.results['failed']}")
        print(f"  Success Rate: {success_rate:.1f}%")
        print(f"  Average Latency: {avg_latency * 1000:.2f} ms")
        print(f"  Throughput: {throughput:.1f} requests/second")
        
        return {
            'duration': duration,
            'total_requests': self.results['sent'],
            'success_rate': success_rate,
            'avg_latency_ms': avg_latency * 1000,
            'throughput_rps': throughput
        }

if __name__ == '__main__':
    tester = LoadTester()
    tester.run_load_test(num_requests=500, num_threads=20)
