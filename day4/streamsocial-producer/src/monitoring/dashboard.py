import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
import json
import requests
from collections import deque
import dash_bootstrap_components as dbc

class ProducerDashboard:
    def __init__(self, producer_metrics_url: str = "http://localhost:8080/metrics"):
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.metrics_url = producer_metrics_url
        self.metrics_history = deque(maxlen=300)  # Keep 5 minutes of data (1 point per second)
        self.running = False
        self.setup_layout()
        self.setup_callbacks()

    def setup_layout(self):
        """Setup the dashboard layout"""
        self.app.layout = dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.H1("StreamSocial Producer Dashboard", className="text-center mb-4"),
                    html.Hr()
                ])
            ]),
            
            # Key Metrics Cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Throughput", className="card-title"),
                            html.H2(id="throughput-value", className="text-primary"),
                            html.P("messages/second", className="text-muted")
                        ])
                    ])
                ], md=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Success Rate", className="card-title"),
                            html.H2(id="success-rate-value", className="text-success"),
                            html.P("percentage", className="text-muted")
                        ])
                    ])
                ], md=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Avg Latency", className="card-title"),
                            html.H2(id="latency-value", className="text-warning"),
                            html.P("milliseconds", className="text-muted")
                        ])
                    ])
                ], md=3),
                
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Queue Size", className="card-title"),
                            html.H2(id="queue-size-value", className="text-info"),
                            html.P("pending messages", className="text-muted")
                        ])
                    ])
                ], md=3),
            ], className="mb-4"),
            
            # Charts
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="throughput-chart")
                ], md=6),
                dbc.Col([
                    dcc.Graph(id="latency-chart")
                ], md=6),
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="success-rate-chart")
                ], md=6),
                dbc.Col([
                    dcc.Graph(id="resource-usage-chart")
                ], md=6),
            ]),
            
            # Auto-refresh interval
            dcc.Interval(
                id='interval-component',
                interval=1000,  # Update every second
                n_intervals=0
            ),
            
        ], fluid=True)

    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        
        @self.app.callback(
            [Output('throughput-value', 'children'),
             Output('success-rate-value', 'children'),
             Output('latency-value', 'children'),
             Output('queue-size-value', 'children'),
             Output('throughput-chart', 'figure'),
             Output('latency-chart', 'figure'),
             Output('success-rate-chart', 'figure'),
             Output('resource-usage-chart', 'figure')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_metrics(n):
            try:
                # Fetch metrics from producer
                response = requests.get(self.metrics_url, timeout=1)
                metrics = response.json()
                
                # Add timestamp
                metrics['timestamp'] = datetime.now()
                self.metrics_history.append(metrics)
                
                # Current values
                throughput = f"{metrics.get('throughput_msg_sec', 0):,.0f}"
                success_rate = f"{metrics.get('success_rate', 0):.1f}%"
                latency = f"{metrics.get('avg_latency_ms', 0):.1f}"
                queue_size = f"{metrics.get('queue_size', 0):,}"
                
                # Create charts
                df = pd.DataFrame(self.metrics_history)
                
                # Throughput chart
                throughput_fig = go.Figure()
                throughput_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['throughput_msg_sec'],
                    mode='lines+markers',
                    name='Messages/sec',
                    line=dict(color='#007bff')
                ))
                throughput_fig.update_layout(
                    title="Message Throughput",
                    xaxis_title="Time",
                    yaxis_title="Messages/Second",
                    height=300
                )
                
                # Latency chart
                latency_fig = go.Figure()
                latency_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['avg_latency_ms'],
                    mode='lines+markers',
                    name='Latency (ms)',
                    line=dict(color='#ffc107')
                ))
                latency_fig.update_layout(
                    title="Average Latency",
                    xaxis_title="Time",
                    yaxis_title="Milliseconds",
                    height=300
                )
                
                # Success rate chart
                success_fig = go.Figure()
                success_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['success_rate'],
                    mode='lines+markers',
                    name='Success Rate (%)',
                    line=dict(color='#28a745')
                ))
                success_fig.update_layout(
                    title="Success Rate",
                    xaxis_title="Time",
                    yaxis_title="Percentage",
                    height=300,
                    yaxis=dict(range=[0, 100])
                )
                
                # Resource usage chart
                resource_fig = go.Figure()
                resource_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['cpu_percent'],
                    mode='lines+markers',
                    name='CPU %',
                    line=dict(color='#dc3545')
                ))
                resource_fig.add_trace(go.Scatter(
                    x=df['timestamp'], 
                    y=df['memory_mb'],
                    mode='lines+markers',
                    name='Memory (MB)',
                    yaxis='y2',
                    line=dict(color='#6f42c1')
                ))
                resource_fig.update_layout(
                    title="Resource Usage",
                    xaxis_title="Time",
                    yaxis_title="CPU %",
                    yaxis2=dict(
                        title="Memory (MB)",
                        overlaying='y',
                        side='right'
                    ),
                    height=300
                )
                
                return (throughput, success_rate, latency, queue_size,
                       throughput_fig, latency_fig, success_fig, resource_fig)
                
            except Exception as e:
                print(f"Error updating metrics: {e}")
                # Return empty values on error
                empty_fig = go.Figure()
                return "0", "0%", "0", "0", empty_fig, empty_fig, empty_fig, empty_fig

    def run(self, host='0.0.0.0', port=8050, debug=False):
        """Run the dashboard"""
        print(f"🚀 Starting Producer Dashboard at http://{host}:{port}")
        self.app.run_server(host=host, port=port, debug=debug)

if __name__ == '__main__':
    dashboard = ProducerDashboard()
    dashboard.run(debug=True)
