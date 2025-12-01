#!/usr/bin/env python3
import sys
import time
import subprocess
import threading
from src.utils.data_generator import InteractionDataGenerator

def run_generator():
    """Run data generator"""
    time.sleep(5)  # Wait for processor to start
    print("\n=== Starting Data Generation ===")
    generator = InteractionDataGenerator('localhost:9092', 'user-interactions')
    generator.generate_events(count=500, rate=50)

def main():
    print("=== StreamSocial KStream Processor Demo ===\n")
    print("This demo will:")
    print("1. Start the stream processor")
    print("2. Generate sample interaction events")
    print("3. Display processing metrics on dashboard")
    print("\nDashboard: http://localhost:8080")
    print("\nPress Ctrl+C to stop\n")
    
    # Start generator in background
    generator_thread = threading.Thread(target=run_generator, daemon=True)
    generator_thread.start()
    
    # Start main processor (blocking)
    from src.main import main as start_processor
    start_processor()

if __name__ == '__main__':
    main()
