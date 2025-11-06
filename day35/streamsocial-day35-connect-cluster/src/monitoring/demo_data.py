"""
Demo data generator for dashboard testing
"""
from datetime import datetime


def get_demo_metrics():
    """Generate demo metrics to show dashboard functionality"""
    return {
        'timestamp': datetime.now().isoformat(),
        'cluster_health': 'healthy',
        'workers': {
            '8083': {
                'url': 'http://localhost:8083',
                'status': 'healthy',
                'version': '7.6.0',
                'commit': 'demo'
            },
            '8084': {
                'url': 'http://localhost:8084',
                'status': 'healthy',
                'version': '7.6.0',
                'commit': 'demo'
            },
            '8085': {
                'url': 'http://localhost:8085',
                'status': 'healthy',
                'version': '7.6.0',
                'commit': 'demo'
            }
        },
        'connectors': {
            'streamsocial-posts-source': {
                'name': 'streamsocial-posts-source',
                'state': 'RUNNING',
                'worker_id': 'connect-worker-1:8083',
                'tasks': [
                    {'id': 0, 'state': 'RUNNING', 'worker_id': 'connect-worker-1:8083', 'trace': ''},
                    {'id': 1, 'state': 'RUNNING', 'worker_id': 'connect-worker-2:8083', 'trace': ''},
                    {'id': 2, 'state': 'RUNNING', 'worker_id': 'connect-worker-3:8083', 'trace': ''}
                ]
            },
            'streamsocial-comments-source': {
                'name': 'streamsocial-comments-source',
                'state': 'RUNNING',
                'worker_id': 'connect-worker-2:8083',
                'tasks': [
                    {'id': 0, 'state': 'RUNNING', 'worker_id': 'connect-worker-1:8083', 'trace': ''},
                    {'id': 1, 'state': 'RUNNING', 'worker_id': 'connect-worker-2:8083', 'trace': ''}
                ]
            },
            'streamsocial-media-source': {
                'name': 'streamsocial-media-source',
                'state': 'RUNNING',
                'worker_id': 'connect-worker-3:8083',
                'tasks': [
                    {'id': 0, 'state': 'RUNNING', 'worker_id': 'connect-worker-3:8083', 'trace': ''}
                ]
            }
        },
        'tasks': {
            'streamsocial-posts-source-0': {
                'id': 0,
                'state': 'RUNNING',
                'worker_id': 'connect-worker-1:8083',
                'trace': ''
            },
            'streamsocial-posts-source-1': {
                'id': 1,
                'state': 'RUNNING',
                'worker_id': 'connect-worker-2:8083',
                'trace': ''
            },
            'streamsocial-posts-source-2': {
                'id': 2,
                'state': 'RUNNING',
                'worker_id': 'connect-worker-3:8083',
                'trace': ''
            },
            'streamsocial-comments-source-0': {
                'id': 0,
                'state': 'RUNNING',
                'worker_id': 'connect-worker-1:8083',
                'trace': ''
            },
            'streamsocial-comments-source-1': {
                'id': 1,
                'state': 'RUNNING',
                'worker_id': 'connect-worker-2:8083',
                'trace': ''
            },
            'streamsocial-media-source-0': {
                'id': 0,
                'state': 'RUNNING',
                'worker_id': 'connect-worker-3:8083',
                'trace': ''
            }
        },
        'rebalance_events': [
            {
                'timestamp': datetime.now().isoformat(),
                'old_distribution': {
                    'streamsocial-posts-source-0': 'connect-worker-1:8083',
                    'streamsocial-posts-source-1': 'connect-worker-1:8083',
                    'streamsocial-posts-source-2': 'connect-worker-1:8083'
                },
                'new_distribution': {
                    'streamsocial-posts-source-0': 'connect-worker-1:8083',
                    'streamsocial-posts-source-1': 'connect-worker-2:8083',
                    'streamsocial-posts-source-2': 'connect-worker-3:8083'
                }
            }
        ],
        'rebalance_count': 1
    }

