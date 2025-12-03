from flask import Flask, render_template, jsonify
from kafka import KafkaConsumer, TopicPartition
import json
import threading
from collections import deque
from datetime import datetime
import os
import time

# Get the project root directory (parent of src/)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
template_dir = os.path.join(project_root, 'templates')

app = Flask(__name__, template_folder=template_dir)

# Store trending data in memory
trending_data = deque(maxlen=100)
trending_history = deque(maxlen=1000)

def kafka_consumer_thread():
    """Background thread to consume trending updates"""
    # First, load all historical messages with a unique consumer group
    try:
        print("Loading historical trending data...", flush=True)
        # Use a unique consumer group to read from beginning
        unique_group = f'dashboard-historical-{int(time.time())}'
        historical_consumer = KafkaConsumer(
            'streamsocial.trending',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000,  # Increased timeout to read all messages
            enable_auto_commit=False,
            group_id=unique_group
        )
        
        print(f"Consumer created, reading messages with timeout 10s...", flush=True)
        message_count = 0
        start_time = time.time()
        for message in historical_consumer:
            if message_count % 50 == 0:
                print(f"Read {message_count} messages so far...", flush=True)
            try:
                trending = message.value
                trending_data.append(trending)
                trending_history.append({
                    'timestamp': datetime.now().isoformat(),
                    'hashtag': trending['hashtag'],
                    'score': trending['score']
                })
                message_count += 1
            except Exception as e:
                print(f"Error processing historical message: {e}", flush=True)
                continue
        
        historical_consumer.close()
        print(f"Loaded {message_count} historical trending updates", flush=True)
    except Exception as e:
        print(f"Error loading historical data: {e}", flush=True)
        import traceback
        traceback.print_exc()
    
    # Now consume new messages with a consumer group - continuous polling
    consumer = None
    while True:
        try:
            if consumer is None:
                # Use a unique consumer group each time to avoid offset issues
                unique_group = f'dashboard-live-{int(time.time())}'
                consumer = KafkaConsumer(
                    'streamsocial.trending',
                    bootstrap_servers='localhost:9092',
                    auto_offset_reset='latest',  # Only new messages
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=1000,
                    group_id=unique_group,
                    enable_auto_commit=False  # Don't commit to avoid offset issues
                )
                print(f"Listening for new trending updates (group: {unique_group})...", flush=True)
            
            # Poll for messages continuously
            message_pack = consumer.poll(timeout_ms=2000, max_records=500)
            
            if message_pack:
                message_count = 0
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        try:
                            trending = message.value
                            trending_data.append(trending)
                            trending_history.append({
                                'timestamp': datetime.now().isoformat(),
                                'hashtag': trending['hashtag'],
                                'score': trending['score']
                            })
                            message_count += 1
                            if message_count <= 5:  # Log first few
                                print(f"New trending update: {trending['hashtag']} (rank {trending.get('rank', 'N/A')}, score {trending.get('score', 0):.3f})", flush=True)
                        except Exception as e:
                            print(f"Error processing message: {e}", flush=True)
                            import traceback
                            traceback.print_exc()
                            continue
                if message_count > 5:
                    print(f"Received {message_count} trending updates", flush=True)
            
        except Exception as e:
            print(f"Error in Kafka consumer thread: {e}", flush=True)
            import traceback
            traceback.print_exc()
            if consumer:
                try:
                    consumer.close()
                except:
                    pass
                consumer = None
            print("Reconnecting in 2 seconds...", flush=True)
            time.sleep(2)

@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/trending')
def get_trending():
    """Return current top trending hashtags"""
    # Sort by rank and return top 20
    sorted_trending = sorted(trending_data, key=lambda x: x.get('rank', 999))
    return jsonify(list(sorted_trending)[:20])

@app.route('/api/history')
def get_history():
    """Return trending history for visualization"""
    return jsonify(list(trending_history))

if __name__ == '__main__':
    # Start Kafka consumer in background
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    app.run(host='0.0.0.0', port=5000, debug=False)
