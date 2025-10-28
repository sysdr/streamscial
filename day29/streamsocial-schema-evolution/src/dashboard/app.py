import dash
from dash import dcc, html, Input, Output, callback_context
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import json
import time
import threading
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from registry.schema_client import SchemaRegistryClient
from consumer.profile_consumer import StreamSocialConsumer
from producer.profile_producer import StreamSocialProducer

# Initialize Dash app with modern theme
app = dash.Dash(__name__, external_stylesheets=[
    'https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css',
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css'
])

# Global data store
data_store = {
    'profiles': [],
    'schema_stats': {'v1': 0, 'v2': 0, 'v3': 0},
    'compatibility_status': {},
    'registry_info': {},
    'last_update': datetime.now().isoformat()
}

# Background data collection
def background_data_collection():
    """Collect data in background"""
    global data_store
    while True:
        try:
            # Update registry info
            registry = SchemaRegistryClient()
            try:
                subjects = registry.list_subjects()
                data_store['registry_info'] = {
                    'subjects': len(subjects),
                    'subjects_list': subjects
                }
                
                if 'user-profile' in subjects:
                    latest = registry.get_latest_schema('user-profile')
                    data_store['registry_info']['latest_version'] = latest.version
                    data_store['registry_info']['compatibility'] = latest.compatibility
                    
            except Exception as e:
                print(f"Registry update error: {e}")
            
            # Update consumer stats
            try:
                consumer = StreamSocialConsumer(group_id=f"dashboard-{int(time.time())}")
                profiles = consumer.consume_profiles(timeout_sec=5)
                consumer.close()
                
                if profiles:
                    data_store['profiles'].extend(profiles)
                    # Keep only last 100 profiles
                    data_store['profiles'] = data_store['profiles'][-100:]
                    
                    # Update stats
                    for profile in profiles:
                        version = profile.get('version', 'v1')
                        data_store['schema_stats'][version] += 1
                        
            except Exception as e:
                print(f"Consumer update error: {e}")
                
            data_store['last_update'] = datetime.now().isoformat()
            time.sleep(10)  # Update every 10 seconds
            
        except Exception as e:
            print(f"Background collection error: {e}")
            time.sleep(5)

# Start background thread
threading.Thread(target=background_data_collection, daemon=True).start()

# Custom CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>StreamSocial Schema Evolution Dashboard</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 0;
            }
            .dashboard-container {
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(10px);
                border-radius: 20px;
                margin: 20px;
                padding: 30px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            }
            .metric-card {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border-radius: 15px;
                padding: 25px;
                text-align: center;
                box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
                transition: transform 0.3s ease;
            }
            .metric-card:hover {
                transform: translateY(-5px);
            }
            .metric-number {
                font-size: 2.5rem;
                font-weight: bold;
                margin-bottom: 10px;
            }
            .metric-label {
                font-size: 1.1rem;
                opacity: 0.9;
            }
            .chart-container {
                background: white;
                border-radius: 15px;
                padding: 25px;
                margin-top: 20px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            }
            .status-badge {
                display: inline-block;
                padding: 8px 16px;
                border-radius: 20px;
                font-weight: bold;
                text-transform: uppercase;
                font-size: 0.8rem;
            }
            .status-compatible {
                background: #d4edda;
                color: #155724;
            }
            .status-incompatible {
                background: #f8d7da;
                color: #721c24;
            }
            .header-title {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                font-size: 2.5rem;
                font-weight: bold;
                text-align: center;
                margin-bottom: 30px;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Layout
app.layout = html.Div([
    dcc.Interval(
        id='interval-component',
        interval=5000,  # Update every 5 seconds
        n_intervals=0
    ),
    
    html.Div(className='dashboard-container', children=[
        html.H1("StreamSocial Schema Evolution Dashboard", className='header-title'),
        
        # Metrics Row
        html.Div(className='row mb-4', children=[
            html.Div(className='col-md-3', children=[
                html.Div(className='metric-card', children=[
                    html.Div(id='total-profiles', className='metric-number'),
                    html.Div("Total Profiles", className='metric-label')
                ])
            ]),
            html.Div(className='col-md-3', children=[
                html.Div(className='metric-card', children=[
                    html.Div(id='registry-subjects', className='metric-number'),
                    html.Div("Registry Subjects", className='metric-label')
                ])
            ]),
            html.Div(className='col-md-3', children=[
                html.Div(className='metric-card', children=[
                    html.Div(id='schema-version', className='metric-number'),
                    html.Div("Latest Schema", className='metric-label')
                ])
            ]),
            html.Div(className='col-md-3', children=[
                html.Div(className='metric-card', children=[
                    html.Div(id='compatibility-mode', className='metric-number'),
                    html.Div("Compatibility", className='metric-label')
                ])
            ])
        ]),
        
        # Charts Row
        html.Div(className='row', children=[
            html.Div(className='col-md-6', children=[
                html.Div(className='chart-container', children=[
                    html.H4("Schema Version Distribution", style={'color': '#333', 'marginBottom': '20px'}),
                    dcc.Graph(id='schema-distribution-chart')
                ])
            ]),
            html.Div(className='col-md-6', children=[
                html.Div(className='chart-container', children=[
                    html.H4("Profile Processing Timeline", style={'color': '#333', 'marginBottom': '20px'}),
                    dcc.Graph(id='timeline-chart')
                ])
            ])
        ]),
        
        # Profile Data Table
        html.Div(className='chart-container mt-4', children=[
            html.H4("Recent Profiles", style={'color': '#333', 'marginBottom': '20px'}),
            html.Div(id='profiles-table')
        ]),
        
        # Control Panel
        html.Div(className='chart-container mt-4', children=[
            html.H4("Schema Evolution Controls", style={'color': '#333', 'marginBottom': '20px'}),
            html.Div(className='row', children=[
                html.Div(className='col-md-4', children=[
                    html.Button('Produce Sample Data', 
                              id='produce-btn', 
                              className='btn btn-primary btn-lg w-100',
                              style={'background': 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', 'border': 'none'})
                ]),
                html.Div(className='col-md-4', children=[
                    html.Button('Test Compatibility', 
                              id='test-btn', 
                              className='btn btn-success btn-lg w-100')
                ]),
                html.Div(className='col-md-4', children=[
                    html.Button('Reset Data', 
                              id='reset-btn', 
                              className='btn btn-warning btn-lg w-100')
                ])
            ]),
            html.Div(id='control-output', className='mt-3')
        ])
    ])
])

# Callbacks
@app.callback(
    [Output('total-profiles', 'children'),
     Output('registry-subjects', 'children'),
     Output('schema-version', 'children'),
     Output('compatibility-mode', 'children'),
     Output('schema-distribution-chart', 'figure'),
     Output('timeline-chart', 'figure'),
     Output('profiles-table', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    global data_store
    
    # Metrics
    total_profiles = len(data_store['profiles'])
    registry_subjects = data_store['registry_info'].get('subjects', 0)
    schema_version = f"V{data_store['registry_info'].get('latest_version', 'N/A')}"
    compatibility_mode = data_store['registry_info'].get('compatibility', 'N/A')
    
    # Schema distribution chart
    stats = data_store['schema_stats']
    fig_dist = px.pie(
        values=[stats['v1'], stats['v2'], stats['v3']],
        names=['Schema V1', 'Schema V2', 'Schema V3'],
        color_discrete_sequence=['#ff7675', '#74b9ff', '#00b894']
    )
    fig_dist.update_traces(textposition='inside', textinfo='percent+label')
    fig_dist.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(size=12)
    )
    
    # Timeline chart
    if data_store['profiles']:
        df = pd.DataFrame(data_store['profiles'][-20:])  # Last 20 profiles
        fig_timeline = px.scatter(
            df, 
            x=range(len(df)), 
            y='version',
            color='is_premium',
            size_max=10,
            color_discrete_map={True: '#00b894', False: '#ddd'}
        )
        fig_timeline.update_layout(
            xaxis_title="Profile Sequence",
            yaxis_title="Schema Version",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
    else:
        fig_timeline = go.Figure()
        fig_timeline.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
    
    # Profiles table
    if data_store['profiles']:
        recent_profiles = data_store['profiles'][-5:]
        table_data = []
        for profile in recent_profiles:
            table_data.append(html.Tr([
                html.Td(profile.get('user_id', 'N/A')),
                html.Td(profile.get('username', 'N/A')),
                html.Td(html.Span(profile.get('version', 'v1').upper(), 
                               className='badge bg-primary')),
                html.Td('✅' if profile.get('has_avatar', False) else '❌'),
                html.Td('💎' if profile.get('is_premium', False) else '👤'),
                html.Td(profile.get('premium_badge', 'None'))
            ]))
        
        profiles_table = html.Table(className='table table-striped', children=[
            html.Thead([
                html.Tr([
                    html.Th("User ID"),
                    html.Th("Username"),
                    html.Th("Schema"),
                    html.Th("Avatar"),
                    html.Th("Premium"),
                    html.Th("Badge")
                ])
            ]),
            html.Tbody(table_data)
        ])
    else:
        profiles_table = html.P("No profiles available yet. Click 'Produce Sample Data' to generate some!")
    
    return (total_profiles, registry_subjects, schema_version, compatibility_mode, 
            fig_dist, fig_timeline, profiles_table)

# Control callbacks
@app.callback(
    Output('control-output', 'children'),
    [Input('produce-btn', 'n_clicks'),
     Input('test-btn', 'n_clicks'),
     Input('reset-btn', 'n_clicks')]
)
def handle_controls(produce_clicks, test_clicks, reset_clicks):
    global data_store
    
    ctx = callback_context
    if not ctx.triggered:
        return ""
    
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'produce-btn':
        try:
            # Run producer in background
            def run_producer():
                producer = StreamSocialProducer()
                producer.produce_profiles(20)
                producer.close()
            
            threading.Thread(target=run_producer, daemon=True).start()
            return html.Div([
                html.I(className="fas fa-check-circle text-success me-2"),
                "Producing 20 sample profiles... Check the dashboard for updates!"
            ], className="alert alert-success")
            
        except Exception as e:
            return html.Div([
                html.I(className="fas fa-exclamation-triangle text-danger me-2"),
                f"Error: {str(e)}"
            ], className="alert alert-danger")
    
    elif button_id == 'test-btn':
        try:
            registry = SchemaRegistryClient()
            v3_schema = open("src/schemas/user_profile_v3.json").read()
            compatible = registry.check_compatibility("user-profile", v3_schema)
            
            return html.Div([
                html.I(className="fas fa-flask text-info me-2"),
                f"Schema V3 compatibility: {'✅ Compatible' if compatible else '❌ Incompatible'}"
            ], className="alert alert-info")
            
        except Exception as e:
            return html.Div([
                html.I(className="fas fa-exclamation-triangle text-danger me-2"),
                f"Test error: {str(e)}"
            ], className="alert alert-danger")
    
    elif button_id == 'reset-btn':
        data_store['profiles'] = []
        data_store['schema_stats'] = {'v1': 0, 'v2': 0, 'v3': 0}
        
        return html.Div([
            html.I(className="fas fa-refresh text-warning me-2"),
            "Dashboard data reset successfully!"
        ], className="alert alert-warning")
    
    return ""

if __name__ == '__main__':
    print("🎨 Starting StreamSocial Schema Evolution Dashboard...")
    print("📍 Dashboard URL: http://localhost:8050")
    app.run_server(debug=False, host='0.0.0.0', port=8050)
