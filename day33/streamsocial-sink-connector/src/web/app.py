"""
StreamSocial Hashtag Analytics Dashboard
Real-time monitoring for sink connector
"""
import os
import json
import time
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
import plotly.graph_objs as go
import plotly.utils
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-secret-key'

# Database connection
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/streamsocial')
engine = create_engine(DATABASE_URL)

@app.route('/')
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/trending')
def get_trending_hashtags():
    """Get current trending hashtags"""
    limit = request.args.get('limit', 50, type=int)
    
    query = """
    SELECT hashtag, count, trend_score, last_updated
    FROM trending_hashtags
    ORDER BY trend_score DESC, count DESC
    LIMIT :limit
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {'limit': limit})
            hashtags = [
                {
                    'hashtag': row.hashtag,
                    'count': row.count,
                    'trend_score': float(row.trend_score or 0),
                    'last_updated': row.last_updated.isoformat() if row.last_updated else None
                }
                for row in result
            ]
        return jsonify({'hashtags': hashtags, 'timestamp': datetime.now().isoformat()})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Get overall statistics"""
    query = """
    SELECT 
        COUNT(*) as total_hashtags,
        SUM(count) as total_mentions,
        AVG(count) as avg_mentions,
        MAX(count) as max_mentions,
        MAX(last_updated) as latest_update
    FROM trending_hashtags
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).first()
            if result:
                stats = {
                    'total_hashtags': result.total_hashtags or 0,
                    'total_mentions': result.total_mentions or 0,
                    'avg_mentions': float(result.avg_mentions or 0),
                    'max_mentions': result.max_mentions or 0,
                    'latest_update': result.latest_update.isoformat() if result.latest_update else None
                }
            else:
                stats = {
                    'total_hashtags': 0,
                    'total_mentions': 0,
                    'avg_mentions': 0,
                    'max_mentions': 0,
                    'latest_update': None
                }
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/top-hashtags')
def top_hashtags_chart():
    """Generate chart data for top hashtags"""
    limit = request.args.get('limit', 20, type=int)
    
    query = """
    SELECT hashtag, count, trend_score
    FROM trending_hashtags
    ORDER BY trend_score DESC, count DESC
    LIMIT :limit
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {'limit': limit})
            data = list(result)
            
        if not data:
            return jsonify({'chartJSON': json.dumps({'data': [], 'layout': {}})})
            
        hashtags = [f"#{row.hashtag}" for row in data]
        counts = [row.count for row in data]
        
        fig = go.Figure(data=[
            go.Bar(
                x=hashtags,
                y=counts,
                marker_color='#1DA1F2',
                text=counts,
                textposition='auto',
            )
        ])
        
        fig.update_layout(
            title='Top Trending Hashtags',
            xaxis_title='Hashtags',
            yaxis_title='Mention Count',
            template='plotly_white',
            height=400
        )
        
        chartJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return jsonify({'chartJSON': chartJSON})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
