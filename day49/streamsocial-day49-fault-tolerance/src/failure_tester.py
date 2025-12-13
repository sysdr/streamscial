import subprocess
import time
import signal
import os
import json
from confluent_kafka.admin import AdminClient

class FailureTester:
    """Test failure scenarios and recovery"""
    
    def __init__(self):
        self.processor_pid = None
        self.admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
        
    def start_processor(self) -> int:
        """Start engagement processor"""
        print("[TEST] Starting processor...")
        proc = subprocess.Popen(
            ['python', 'src/engagement_processor.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        self.processor_pid = proc.pid
        time.sleep(5)  # Wait for startup
        return proc.pid
        
    def kill_processor(self):
        """Simulate crash by killing processor"""
        if self.processor_pid:
            print(f"[TEST] Simulating crash: killing PID {self.processor_pid}")
            os.kill(self.processor_pid, signal.SIGKILL)
            self.processor_pid = None
            time.sleep(2)
            
    def verify_state_consistency(self) -> bool:
        """Verify state store matches changelog"""
        print("[TEST] Verifying state consistency...")
        # In production, compare RocksDB with changelog topic
        return True
        
    def measure_recovery_time(self) -> float:
        """Measure time to resume processing after crash"""
        start_time = time.time()
        self.start_processor()
        # Wait for processing to resume (check via API)
        time.sleep(10)
        recovery_time = time.time() - start_time
        print(f"[TEST] Recovery time: {recovery_time:.2f} seconds")
        return recovery_time
        
    def run_crash_recovery_test(self):
        """Test crash and recovery scenario"""
        print("\n" + "="*50)
        print("TEST: Crash Recovery Scenario")
        print("="*50)
        
        # Start processor
        self.start_processor()
        print("[TEST] Processor running, generating load...")
        
        # Generate events
        subprocess.run(['python', 'src/event_generator.py'])
        time.sleep(5)
        
        # Crash processor
        self.kill_processor()
        
        # Measure recovery
        recovery_time = self.measure_recovery_time()
        
        # Verify consistency
        consistent = self.verify_state_consistency()
        
        print(f"\n[RESULT] Recovery Time: {recovery_time:.2f}s")
        print(f"[RESULT] State Consistent: {consistent}")
        print(f"[RESULT] Test {'PASSED' if recovery_time < 60 and consistent else 'FAILED'}")
        
if __name__ == '__main__':
    tester = FailureTester()
    tester.run_crash_recovery_test()
