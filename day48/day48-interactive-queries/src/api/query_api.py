from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from flask_socketio import SocketIO
import time
import threading
import requests
from collections import defaultdict
from typing import List, Dict
import json
import os

class InteractiveQueryAPI:
    """REST API for querying Kafka Streams state"""
    
    def __init__(self, processor, peer_instances: List[Dict]):
        self.app = Flask(__name__)
        CORS(self.app)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        
        self.processor = processor
        self.peer_instances = peer_instances
        self.window_size_ms = processor.window_size_ms
        
        self._setup_routes()
        self._start_live_updates()
        
    def _setup_routes(self):
        """Setup API routes"""
        
        @self.app.route('/api/health', methods=['GET'])
        def health():
            """Health check endpoint"""
            return jsonify({
                'status': 'healthy',
                'instance': self.processor.get_metadata(),
                'timestamp': int(time.time() * 1000)
            })
        
        @self.app.route('/api/trending', methods=['GET'])
        def get_trending():
            """Get top trending hashtags"""
            limit = int(request.args.get('limit', 10))
            windows = int(request.args.get('windows', 3))
            
            # Query local and remote stores
            all_trends = self._query_all_instances(windows)
            
            # Rank by total count
            ranked = sorted(all_trends.items(), key=lambda x: x[1]['total_count'], reverse=True)
            
            result = []
            for hashtag, data in ranked[:limit]:
                result.append({
                    'hashtag': hashtag,
                    'count': data['total_count'],
                    'velocity': data['velocity'],
                    'windows': data['windows'],
                    'trend': 'up' if data['velocity'] > 0 else 'down'
                })
            
            return jsonify({
                'trending': result,
                'timestamp': int(time.time() * 1000),
                'window_size_minutes': self.window_size_ms // (60 * 1000)
            })
        
        @self.app.route('/api/trending/<hashtag>', methods=['GET'])
        def get_hashtag_stats(hashtag):
            """Get stats for specific hashtag"""
            windows = int(request.args.get('windows', 6))
            
            stats = self._query_hashtag(hashtag.lower(), windows)
            
            return jsonify({
                'hashtag': hashtag,
                'stats': stats,
                'timestamp': int(time.time() * 1000)
            })
        
        @self.app.route('/api/metadata', methods=['GET'])
        def get_metadata():
            """Get cluster metadata"""
            instances = [self.processor.get_metadata()]
            for peer in self.peer_instances:
                try:
                    response = requests.get(
                        f"http://{peer['host']}:{peer['port']}/api/health",
                        timeout=1
                    )
                    instances.append(response.json()['instance'])
                except:
                    pass
            
            return jsonify({
                'instances': instances,
                'total_instances': len(instances)
            })
        
        @self.app.route('/api/query/local', methods=['POST'])
        def query_local():
            """Query local store - used by peer instances"""
            data = request.json
            time_from = data.get('time_from', 0)
            time_to = data.get('time_to', int(time.time() * 1000))
            
            results = self._query_local_store(time_from, time_to)
            return jsonify(results)
        
        @self.app.route('/web/dashboard.html', methods=['GET'])
        def serve_dashboard():
            """Serve dashboard HTML"""
            # Get path relative to project root
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(script_dir)
            dashboard_path = os.path.join(project_root, 'src', 'web', 'dashboard.html')
            return send_file(dashboard_path)
    
    def _query_all_instances(self, num_windows: int) -> Dict:
        """Query all instances and merge results"""
        current_time = int(time.time() * 1000)
        time_from = current_time - (num_windows * self.window_size_ms)
        time_to = current_time
        
        # Query local store
        local_results = self._query_local_store(time_from, time_to)
        
        # Query remote stores (if any)
        for peer in self.peer_instances:
            try:
                response = requests.post(
                    f"http://{peer['host']}:{peer['port']}/api/query/local",
                    json={'time_from': time_from, 'time_to': time_to},
                    timeout=1
                )
                remote_results = response.json()
                self._merge_results(local_results, remote_results)
            except:
                pass
        
        # Calculate velocity
        for hashtag, data in local_results.items():
            windows = data['windows']
            if len(windows) >= 2:
                recent_sum = sum(w['count'] for w in windows[-2:])
                older_sum = sum(w['count'] for w in windows[:-2]) if len(windows) > 2 else 0
                data['velocity'] = recent_sum - older_sum
            else:
                data['velocity'] = 0
        
        return local_results
    
    def _query_local_store(self, time_from: int, time_to: int) -> Dict:
        """Query local window store"""
        store = self.processor.get_store()
        all_data = store.fetch_all(time_from, time_to)
        
        results = {}
        for hashtag, windows in all_data.items():
            total_count = sum(count for _, count in windows)
            results[hashtag] = {
                'total_count': total_count,
                'windows': [{'window': w, 'count': c} for w, c in windows],
                'velocity': 0
            }
        
        return results
    
    def _query_hashtag(self, hashtag: str, num_windows: int) -> Dict:
        """Query specific hashtag across time"""
        current_time = int(time.time() * 1000)
        time_from = current_time - (num_windows * self.window_size_ms)
        time_to = current_time
        
        store = self.processor.get_store()
        windows = store.fetch(hashtag, time_from, time_to)
        
        return {
            'windows': [{'window': w, 'count': c, 'timestamp': w} for w, c in windows],
            'total_count': sum(c for _, c in windows),
            'average': sum(c for _, c in windows) / len(windows) if windows else 0
        }
    
    def _merge_results(self, target: Dict, source: Dict):
        """Merge query results from different instances"""
        for hashtag, data in source.items():
            if hashtag not in target:
                target[hashtag] = data
            else:
                target[hashtag]['total_count'] += data['total_count']
                # Merge windows
                window_map = {w['window']: w['count'] for w in target[hashtag]['windows']}
                for w in data['windows']:
                    if w['window'] in window_map:
                        window_map[w['window']] += w['count']
                    else:
                        window_map[w['window']] = w['count']
                target[hashtag]['windows'] = [
                    {'window': w, 'count': c} for w, c in window_map.items()
                ]
    
    def _start_live_updates(self):
        """Start WebSocket live updates"""
        def broadcast_updates():
            while True:
                time.sleep(5)  # Update every 5 seconds
                try:
                    trends = self._query_all_instances(3)
                    ranked = sorted(trends.items(), key=lambda x: x[1]['total_count'], reverse=True)[:10]
                    
                    update = {
                        'trending': [
                            {
                                'hashtag': h,
                                'count': d['total_count'],
                                'velocity': d['velocity']
                            }
                            for h, d in ranked
                        ],
                        'timestamp': int(time.time() * 1000)
                    }
                    
                    self.socketio.emit('trending_update', update)
                except:
                    pass
        
        thread = threading.Thread(target=broadcast_updates, daemon=True)
        thread.start()
    
    def run(self, host='0.0.0.0', port=8080):
        """Run API server"""
        self.socketio.run(self.app, host=host, port=port, allow_unsafe_werkzeug=True)
