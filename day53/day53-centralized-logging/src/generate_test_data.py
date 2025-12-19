"""Generate test data with trace IDs and errors for dashboard testing"""
import json
import time
from datetime import datetime
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent))
from shared.structured_logger import setup_structured_logger, generate_trace_id, LogContext

def generate_test_logs():
    """Generate test logs with trace IDs, errors, and latency data"""
    
    # Setup logger
    logger = setup_structured_logger(
        'test-data-generator',
        'logs/application/producer.log'
    )
    
    print("Generating test data with trace IDs and errors...")
    
    # Generate some successful requests with trace IDs
    for i in range(10):
        trace_id = generate_trace_id()
        start_time = time.time()
        
        with LogContext(logger, trace_id=trace_id, content_id=i+1, views=1000+i*100) as log:
            # Simulate processing time
            time.sleep(0.01)
            latency = (time.time() - start_time) * 1000
            
            log.info("message_delivered",
                    topic="viral-content",
                    partition=0,
                    offset=i,
                    latency_ms=round(latency, 2),
                    message_size=256)
            
            log.info("message_processed",
                    processing_time_ms=round(latency, 2),
                    engagement_score=round(1000 * 0.1, 2))
    
    # Generate some error logs with trace IDs
    error_trace_ids = []
    for i in range(5):
        trace_id = generate_trace_id()
        error_trace_ids.append(trace_id)
        start_time = time.time()
        
        with LogContext(logger, trace_id=trace_id, content_id=100+i) as log:
            time.sleep(0.005)
            latency = (time.time() - start_time) * 1000
            
            # Generate different types of errors
            if i % 2 == 0:
                log.error("processing_error",
                         error="Invalid engagement rate: -1",
                         error_type="ValueError",
                         partition=0,
                         offset=100+i,
                         processing_time_ms=round(latency, 2))
            else:
                log.error("cache_update_failed",
                         error="Connection timeout",
                         error_type="TimeoutError",
                         processing_time_ms=round(latency, 2))
    
    # Generate some warnings
    for i in range(3):
        trace_id = generate_trace_id()
        with LogContext(logger, trace_id=trace_id, content_id=200+i) as log:
            log.warning("cache_slow_response",
                       latency_ms=500)
    
    print(f"\nGenerated test data!")
    print(f"Sample trace IDs to search for:")
    for trace_id in error_trace_ids[:3]:
        print(f"  - {trace_id}")
    
    print("\nTest data written to logs/application/producer.log")
    print("Logstash will process these and send to Elasticsearch.")
    print("Wait a few seconds for Logstash to process, then refresh the dashboard.")

if __name__ == '__main__':
    generate_test_logs()

