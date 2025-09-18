import json
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS
from async_producer import ContentModerationProducer
import yaml
import threading
import time

app = Flask(__name__)
CORS(app)

# Load configuration
with open('config/kafka_config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Initialize producer
producer = ContentModerationProducer(config)

@app.route('/post', methods=['POST'])
def submit_post():
    """Submit content for moderation"""
    try:
        data = request.json
        user_id = data.get('user_id')
        content = data.get('content')
        
        if not user_id or not content:
            return jsonify({'error': 'user_id and content required'}), 400
        
        result = producer.send_for_moderation(user_id, content)
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get producer metrics"""
    return jsonify(producer.get_metrics())

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': time.time()})

@app.route('/logs', methods=['GET'])
def get_logs():
    """Get recent activity logs"""
    metrics = producer.get_metrics()
    logs = [
        f"🔄 Auto-refreshing every 20 seconds...",
        f"📊 Monitoring async producer performance",
        f"✅ Ready to process content moderation requests",
        f"📝 Recent Activity:",
        f"• Messages sent: {metrics['sent']}",
        f"• Successfully delivered: {metrics['delivered']}",
        f"• Currently in flight: {metrics['in_flight']}",
        f"• Failed messages: {metrics['failed']}",
        f"• Retry attempts: {metrics['retried']}",
        f"• Success rate: {(metrics['delivered'] / max(metrics['sent'], 1) * 100):.1f}%",
        f"• Last updated: {time.strftime('%H:%M:%S')}"
    ]
    return jsonify({'logs': logs})

@app.route('/dashboard', methods=['GET'])
def dashboard():
    """Simple HTML dashboard"""
    metrics = producer.get_metrics()
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>StreamSocial Async Producer Dashboard</title>
        <meta http-equiv="refresh" content="20">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            .header {{ background: #1da1f2; color: white; padding: 20px; border-radius: 8px; }}
            .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
            .metric-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .metric-value {{ font-size: 2em; font-weight: bold; color: #1da1f2; }}
            .test-form {{ background: white; padding: 20px; border-radius: 8px; margin: 20px 0; }}
            .test-form input, .test-form textarea {{ width: 100%; padding: 10px; margin: 5px 0; }}
            .test-form button {{ background: #1da1f2; color: white; padding: 10px 20px; border: none; border-radius: 4px; }}
            .log {{ background: #000; color: #0f0; padding: 20px; border-radius: 8px; font-family: monospace; height: 200px; overflow-y: scroll; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 StreamSocial Async Producer Dashboard</h1>
                <p>Content Moderation Pipeline - Day 19</p>
            </div>
            
            <div class="metrics">
                <div class="metric-card">
                    <h3>Messages Sent</h3>
                    <div class="metric-value">{metrics['sent']}</div>
                </div>
                <div class="metric-card">
                    <h3>Successfully Delivered</h3>
                    <div class="metric-value">{metrics['delivered']}</div>
                </div>
                <div class="metric-card">
                    <h3>In Flight</h3>
                    <div class="metric-value">{metrics['in_flight']}</div>
                </div>
                <div class="metric-card">
                    <h3>Failed</h3>
                    <div class="metric-value">{metrics['failed']}</div>
                </div>
                <div class="metric-card">
                    <h3>Retries</h3>
                    <div class="metric-value">{metrics['retried']}</div>
                </div>
                <div class="metric-card">
                    <h3>Success Rate</h3>
                    <div class="metric-value">{(metrics['delivered'] / max(metrics['sent'], 1) * 100):.1f}%</div>
                </div>
            </div>
            
            <div class="test-form">
                <h3>🧪 Test Content Submission</h3>
                <form onsubmit="submitPost(event)">
                    <input type="text" id="userId" placeholder="User ID (e.g., user123 or vip_alice)" required>
                    <textarea id="content" placeholder="Post content..." rows="3" required></textarea>
                    <button type="submit">Submit for Moderation</button>
                </form>
                <div id="result"></div>
            </div>
            
            <div class="log" id="log">
                <div>🔄 Auto-refreshing every 20 seconds...</div>
                <div>📊 Monitoring async producer performance</div>
                <div>✅ Ready to process content moderation requests</div>
                <div>📝 Recent Activity:</div>
                <div>• Messages sent: {metrics['sent']}</div>
                <div>• Successfully delivered: {metrics['delivered']}</div>
                <div>• Currently in flight: {metrics['in_flight']}</div>
                <div>• Failed messages: {metrics['failed']}</div>
                <div>• Retry attempts: {metrics['retried']}</div>
                <div>• Success rate: {(metrics['delivered'] / max(metrics['sent'], 1) * 100):.1f}%</div>
                <div>• Last updated: {time.strftime('%H:%M:%S')}</div>
            </div>
        </div>
        
        <script>
            async function submitPost(event) {{
                event.preventDefault();
                const userId = document.getElementById('userId').value;
                const content = document.getElementById('content').value;
                
                try {{
                    const response = await fetch('/post', {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ user_id: userId, content: content }})
                    }});
                    
                    const result = await response.json();
                    document.getElementById('result').innerHTML = 
                        '<div style="margin-top: 10px; padding: 10px; background: #e8f5e8; border-radius: 4px;">' +
                        'Result: ' + JSON.stringify(result, null, 2) + '</div>';
                    
                    // Update logs after successful submission
                    updateLogs();
                        
                }} catch (error) {{
                    document.getElementById('result').innerHTML = 
                        '<div style="margin-top: 10px; padding: 10px; background: #ffe8e8; border-radius: 4px;">' +
                        'Error: ' + error.message + '</div>';
                }}
            }}
            
            async function updateLogs() {{
                try {{
                    const response = await fetch('/logs');
                    const data = await response.json();
                    const logElement = document.getElementById('log');
                    logElement.innerHTML = data.logs.map(log => `<div>${{log}}</div>`).join('');
                }} catch (error) {{
                    console.error('Failed to update logs:', error);
                }}
            }}
            
            // Update logs every 5 seconds
            setInterval(updateLogs, 5000);
            
            // Initial log update
            updateLogs();
        </script>
    </body>
    </html>
    """
    return html

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
