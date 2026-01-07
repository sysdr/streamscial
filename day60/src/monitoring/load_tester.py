"""
Load Testing Framework
Simulates production load to validate capacity
"""
import time
import json
import random
import threading
from typing import Dict, List
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoadTester:
    def __init__(self, target_rps: int, duration_seconds: int):
        self.target_rps = target_rps
        self.duration_seconds = duration_seconds
        self.metrics = {
            "requests_sent": 0,
            "requests_success": 0,
            "requests_failed": 0,
            "latencies": [],
            "errors": []
        }
        self.running = False
    
    def simulate_request(self) -> Dict:
        """Simulate a single API request"""
        start_time = time.time()
        
        try:
            # Simulate API call latency (production would call real API)
            latency_ms = random.gauss(150, 50)  # Mean 150ms, stddev 50ms
            time.sleep(latency_ms / 1000)
            
            # Simulate success/failure (99.9% success rate)
            success = random.random() > 0.001
            
            if success:
                self.metrics["requests_success"] += 1
                return {
                    "status": "success",
                    "latency_ms": latency_ms
                }
            else:
                self.metrics["requests_failed"] += 1
                return {
                    "status": "error",
                    "latency_ms": latency_ms,
                    "error": "Simulated timeout"
                }
        
        except Exception as e:
            self.metrics["requests_failed"] += 1
            return {
                "status": "error",
                "error": str(e)
            }
    
    def worker_thread(self, requests_per_thread: int):
        """Worker thread to generate load"""
        for _ in range(requests_per_thread):
            if not self.running:
                break
            
            result = self.simulate_request()
            self.metrics["requests_sent"] += 1
            
            if "latency_ms" in result:
                self.metrics["latencies"].append(result["latency_ms"])
            
            if result["status"] == "error":
                self.metrics["errors"].append(result.get("error", "Unknown"))
    
    def run_load_test(self) -> Dict:
        """Execute load test"""
        logger.info(f"Starting load test: {self.target_rps} RPS for {self.duration_seconds}s")
        self.running = True
        start_time = time.time()
        
        # Calculate thread distribution
        num_threads = min(100, self.target_rps)  # Max 100 threads
        requests_per_thread = int((self.target_rps * self.duration_seconds) / num_threads)
        
        # Start worker threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=self.worker_thread, args=(requests_per_thread,))
            t.start()
            threads.append(t)
        
        # Wait for completion
        for t in threads:
            t.join()
        
        self.running = False
        total_time = time.time() - start_time
        
        # Calculate statistics
        latencies = sorted(self.metrics["latencies"])
        
        return {
            "test_parameters": {
                "target_rps": self.target_rps,
                "duration_seconds": self.duration_seconds,
                "actual_duration": round(total_time, 2)
            },
            "results": {
                "total_requests": self.metrics["requests_sent"],
                "successful_requests": self.metrics["requests_success"],
                "failed_requests": self.metrics["requests_failed"],
                "success_rate_percent": round(
                    self.metrics["requests_success"] / self.metrics["requests_sent"] * 100, 3
                ),
                "actual_rps": round(self.metrics["requests_sent"] / total_time, 2)
            },
            "latency": {
                "min_ms": round(min(latencies), 2) if latencies else 0,
                "max_ms": round(max(latencies), 2) if latencies else 0,
                "mean_ms": round(sum(latencies) / len(latencies), 2) if latencies else 0,
                "p50_ms": round(latencies[len(latencies) // 2], 2) if latencies else 0,
                "p95_ms": round(latencies[int(len(latencies) * 0.95)], 2) if latencies else 0,
                "p99_ms": round(latencies[int(len(latencies) * 0.99)], 2) if latencies else 0,
                "p99_9_ms": round(latencies[int(len(latencies) * 0.999)], 2) if latencies else 0
            },
            "errors": self.metrics["errors"][:10]  # First 10 errors
        }

if __name__ == "__main__":
    # Run load test scenarios
    scenarios = [
        {"name": "Normal Load", "rps": 50000, "duration": 10},
        {"name": "Peak Load", "rps": 150000, "duration": 10},
        {"name": "Spike Test", "rps": 300000, "duration": 5}
    ]
    
    for scenario in scenarios:
        print(f"\n{'='*60}")
        print(f"Scenario: {scenario['name']}")
        print(f"{'='*60}")
        
        tester = LoadTester(scenario["rps"], scenario["duration"])
        result = tester.run_load_test()
        
        print(json.dumps(result, indent=2))
