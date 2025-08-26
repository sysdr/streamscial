import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import time
import threading
from typing import Dict, List
import redis
import json
import requests

from config.kafka_config import *

class RebalancingDashboard:
    def __init__(self):
        self.app = dash.Dash(__name__)
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.setup_layout()
        self.setup_callbacks()
        
    def setup_layout(self):
        """Setup dashboard layout"""
        self.app.layout = html.Div([
            html.H1("StreamSocial Rebalancing Dashboard", 
                   style={'textAlign': 'center', 'color': '#2E86AB'}),
            
            html.Div([
                html.Div([
                    html.H3("Consumer Metrics"),
                    dcc.Graph(id='consumer-count-graph'),
                ], className='six columns'),
                
                html.Div([
                    html.H3("Partition Lag"),
                    dcc.Graph(id='partition-lag-graph'),
                ], className='six columns'),
            ], className='row'),
            
            html.Div([
                html.Div([
                    html.H3("Processing Latency"),
                    dcc.Graph(id='latency-graph'),
                ], className='six columns'),
                
                html.Div([
                    html.H3("Rebalancing Events"),
                    dcc.Graph(id='rebalance-events-graph'),
                ], className='six columns'),
            ], className='row'),
            
            html.Div([
                html.H3("Live System Status"),
                html.Div(id='system-status'),
            ], style={'margin': '20px'}),
            
            dcc.Interval(
                id='interval-component',
                interval=2*1000,  # Update every 2 seconds
                n_intervals=0
            )
        ])
        
    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        @self.app.callback(
            [Output('consumer-count-graph', 'figure'),
             Output('partition-lag-graph', 'figure'),
             Output('latency-graph', 'figure'),
             Output('rebalance-events-graph', 'figure'),
             Output('system-status', 'children')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_graphs(n):
            # Get current metrics
            metrics = self._collect_metrics()
            
            # Consumer count graph
            consumer_fig = go.Figure()
            consumer_fig.add_trace(go.Scatter(
                x=metrics['timestamps'],
                y=metrics['consumer_counts'],
                mode='lines+markers',
                name='Active Consumers',
                line=dict(color='#A23B72', width=3)
            ))
            consumer_fig.update_layout(
                title='Active Consumers Over Time',
                xaxis_title='Time',
                yaxis_title='Consumer Count',
                height=300
            )
            
            # Partition lag graph
            lag_fig = go.Figure()
            for partition, lags in metrics['partition_lags'].items():
                lag_fig.add_trace(go.Scatter(
                    x=metrics['timestamps'],
                    y=lags,
                    mode='lines',
                    name=f'Partition {partition}',
                    line=dict(width=2)
                ))
            lag_fig.update_layout(
                title='Partition Lag by Partition',
                xaxis_title='Time',
                yaxis_title='Lag (messages)',
                height=300
            )
            
            # Latency graph
            latency_fig = go.Figure()
            latency_fig.add_trace(go.Scatter(
                x=metrics['timestamps'],
                y=metrics['avg_latencies'],
                mode='lines+markers',
                name='Avg Processing Latency',
                line=dict(color='#F18F01', width=3)
            ))
            latency_fig.update_layout(
                title='Average Processing Latency',
                xaxis_title='Time',
                yaxis_title='Latency (ms)',
                height=300
            )
            
            # Rebalance events graph
            rebalance_fig = go.Figure()
            rebalance_fig.add_trace(go.Bar(
                x=['Revoked', 'Assigned'],
                y=[metrics['rebalance_events']['revoked'], metrics['rebalance_events']['assigned']],
                marker_color=['#C73E1D', '#2E86AB']
            ))
            rebalance_fig.update_layout(
                title='Rebalancing Events (Last Hour)',
                xaxis_title='Event Type',
                yaxis_title='Count',
                height=300
            )
            
            # System status
            status_html = html.Div([
                html.P(f"🟢 Active Consumers: {metrics['current_consumers']}", 
                      style={'fontSize': '18px', 'margin': '5px'}),
                html.P(f"⚡ Total Messages Processed: {metrics['total_processed']:,}", 
                      style={'fontSize': '18px', 'margin': '5px'}),
                html.P(f"🎯 Max Partition Lag: {metrics['max_lag']} messages", 
                      style={'fontSize': '18px', 'margin': '5px'}),
                html.P(f"🔄 Last Rebalance: {metrics['last_rebalance']}", 
                      style={'fontSize': '18px', 'margin': '5px'}),
            ])
            
            return consumer_fig, lag_fig, latency_fig, rebalance_fig, status_html
            
    def _collect_metrics(self) -> Dict:
        """Collect current system metrics"""
        current_time = time.time()
        
        # Simulate metrics collection
        metrics = {
            'timestamps': [current_time - i*10 for i in range(30, 0, -1)],
            'consumer_counts': [3 + (i % 5) for i in range(30)],
            'partition_lags': {
                0: [100 + (i * 10) % 500 for i in range(30)],
                1: [150 + (i * 15) % 600 for i in range(30)],
                2: [80 + (i * 8) % 400 for i in range(30)],
            },
            'avg_latencies': [50 + (i % 10) for i in range(30)],
            'rebalance_events': {'revoked': 5, 'assigned': 5},
            'current_consumers': self._get_current_consumer_count(),
            'total_processed': 145678,
            'max_lag': 450,
            'last_rebalance': '2 minutes ago'
        }
        
        return metrics
        
    def _get_current_consumer_count(self) -> int:
        """Get current consumer count"""
        try:
            with open('/tmp/consumer_count.txt', 'r') as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return 3
            
    def run(self, host='0.0.0.0', port=8050, debug=False):
        """Run the dashboard"""
        self.app.run_server(host=host, port=port, debug=debug)
