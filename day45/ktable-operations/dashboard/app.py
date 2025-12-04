from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import asyncio
import httpx
from config.settings import settings

app = FastAPI(title="Reputation Dashboard")

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return """
<!DOCTYPE html>
<html>
<head>
    <title>StreamSocial Reputation Dashboard</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: #f5f7fa;
            padding: 20px;
            min-height: 100vh;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        header {
            background: #ffffff;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 30px;
            border: 1px solid #e5e7eb;
        }
        
        h1 {
            color: #1f2937;
            font-size: 2.5em;
            font-weight: 700;
            margin-bottom: 10px;
        }
        
        .subtitle {
            color: #6b7280;
            font-size: 1.1em;
        }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .card {
            background: #ffffff;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            border: 1px solid #e5e7eb;
        }
        
        .card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
        }
        
        .metric-value {
            font-size: 2.5em;
            font-weight: 700;
            color: #059669;
            margin: 10px 0;
        }
        
        .metric-label {
            color: #6b7280;
            font-size: 0.9em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .chart-container {
            background: #ffffff;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 30px;
            min-height: 400px;
            border: 1px solid #e5e7eb;
        }
        
        #tierChart {
            max-height: 400px;
        }
        
        .chart-title {
            color: #1f2937;
            font-size: 1.3em;
            font-weight: 700;
            margin-bottom: 20px;
        }
        
        .leaderboard {
            background: #ffffff;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            border: 1px solid #e5e7eb;
        }
        
        .leaderboard-item {
            display: flex;
            align-items: center;
            padding: 15px;
            margin: 10px 0;
            background: #f9fafb;
            border-radius: 8px;
            transition: transform 0.2s, background 0.2s;
            border: 1px solid #e5e7eb;
        }
        
        .leaderboard-item:hover {
            transform: scale(1.01);
            background: #f3f4f6;
        }
        
        .rank {
            font-size: 1.5em;
            font-weight: 700;
            color: #059669;
            min-width: 50px;
        }
        
        .user-id {
            flex-grow: 1;
            font-weight: 600;
            color: #1f2937;
        }
        
        .tier-badge {
            padding: 6px 16px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-right: 10px;
        }
        
        .tier-bronze { background: linear-gradient(135deg, #cd7f32 0%, #b87333 100%); color: white; }
        .tier-silver { background: linear-gradient(135deg, #c0c0c0 0%, #a8a8a8 100%); color: white; }
        .tier-gold { background: linear-gradient(135deg, #ffd700 0%, #ffed4e 100%); color: #1e293b; }
        .tier-platinum { background: linear-gradient(135deg, #e5e4e2 0%, #d3d3d3 100%); color: #1e293b; }
        
        .score {
            font-size: 1.2em;
            font-weight: 700;
            color: #059669;
        }
        
        .status {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #10b981;
            animation: pulse 2s infinite;
            margin-right: 8px;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1><span class="status"></span>StreamSocial Reputation System</h1>
            <div class="subtitle">Real-time KTable Operations & Materialized Views</div>
        </header>
        
        <div class="grid">
            <div class="card">
                <div class="metric-label">Total Users</div>
                <div class="metric-value" id="totalUsers">0</div>
            </div>
            <div class="card">
                <div class="metric-label">Total Reputation Points</div>
                <div class="metric-value" id="totalPoints">0</div>
            </div>
            <div class="card">
                <div class="metric-label">Average Score</div>
                <div class="metric-value" id="avgScore">0</div>
            </div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">Tier Distribution</div>
            <canvas id="tierChart"></canvas>
        </div>
        
        <div class="leaderboard">
            <div class="chart-title">Top Users Leaderboard</div>
            <div id="leaderboard"></div>
        </div>
    </div>
    
    <script>
        let tierChart;
        
        // Plugin to ensure all bars are visible even with zero values
        const minBarHeightPlugin = {
            id: 'minBarHeight',
            afterDraw: (chart) => {
                try {
                    console.log('🔧 Plugin called - afterDraw');
                    const ctx = chart.ctx;
                    const xScale = chart.scales.x;
                    const yScale = chart.scales.y;
                    
                    if (!xScale || !yScale) {
                        console.error('❌ Scales not available');
                        return;
                    }
                    
                    const zeroPoint = yScale.getPixelForValue(0);
                    const dataset = chart.data.datasets[0];
                    
                    if (!dataset || !dataset.data) {
                        console.error('❌ Dataset not available');
                        return;
                    }
                    
                    // Calculate bar dimensions
                    const categoryCount = chart.data.labels.length;
                    const categoryWidth = xScale.width / categoryCount;
                    const barWidth = categoryWidth * 0.8 * 0.9; // Match categoryPercentage and barPercentage
                    const minHeight = 20; // Minimum 20 pixels height for better visibility
                    
                    console.log('📊 Plugin - categoryCount:', categoryCount, 'barWidth:', barWidth, 'zeroPoint:', zeroPoint, 'data:', dataset.data);
                    
                    let drawnCount = 0;
                    dataset.data.forEach((value, index) => {
                        if (value === 0 || value === null || value === undefined) {
                            // Calculate x position manually
                            const x = xScale.getPixelForValue(index);
                            
                            console.log(`🎨 Drawing zero bar for index ${index} (${chart.data.labels[index]}) at x=${x}, value=${value}`);
                            
                            ctx.save();
                            ctx.fillStyle = dataset.backgroundColor[index];
                            ctx.strokeStyle = dataset.borderColor[index];
                            ctx.lineWidth = dataset.borderWidth || 2;
                            
                            // Draw a simple rectangle at the bottom
                            const left = x - barWidth / 2;
                            const top = zeroPoint - minHeight;
                            
                            // Simple rectangle - more reliable
                            ctx.fillRect(left, top, barWidth, minHeight);
                            ctx.strokeRect(left, top, barWidth, minHeight);
                            
                            ctx.restore();
                            drawnCount++;
                        }
                    });
                    if (drawnCount > 0) {
                        console.log(`✅ Drew ${drawnCount} zero-value bars`);
                    } else {
                        console.log('ℹ️ No zero values to draw');
                    }
                } catch (error) {
                    console.error('❌ Error in minBarHeight plugin:', error);
                    console.error(error.stack);
                }
            }
        };
        
        function initChart() {
            const ctx = document.getElementById('tierChart').getContext('2d');
            tierChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: ['Bronze', 'Silver', 'Gold', 'Platinum'],
                    datasets: [{
                        label: 'Users',
                        data: [0, 0, 0, 0],
                        backgroundColor: [
                            'rgba(205, 127, 50, 0.8)',
                            'rgba(192, 192, 192, 0.8)',
                            'rgba(255, 215, 0, 0.8)',
                            'rgba(229, 228, 226, 0.8)'
                        ],
                        borderColor: [
                            'rgba(205, 127, 50, 1)',
                            'rgba(192, 192, 192, 1)',
                            'rgba(255, 215, 0, 1)',
                            'rgba(229, 228, 226, 1)'
                        ],
                        borderWidth: 2,
                        borderRadius: 10
                    }]
                },
                plugins: [minBarHeightPlugin],
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            enabled: true,
                            callbacks: {
                                label: function(context) {
                                    return 'Users: ' + context.parsed.y;
                                }
                            }
                        }
                    },
                    scales: {
                        y: { 
                            beginAtZero: true,
                            min: 0,
                            ticks: {
                                stepSize: 1,
                                precision: 0,
                                callback: function(value) {
                                    return Number.isInteger(value) ? value : '';
                                }
                            },
                            grid: { color: 'rgba(0,0,0,0.08)' }
                        },
                        x: { 
                            grid: { display: false },
                            ticks: {
                                font: {
                                    size: 12,
                                    weight: 'bold'
                                }
                            },
                            display: true
                        }
                    },
                    animation: {
                        duration: 0 // Disable animation to ensure immediate updates
                    },
                    elements: {
                        bar: {
                            borderWidth: 2,
                            borderSkipped: false
                        }
                    },
                    categoryPercentage: 0.8,
                    barPercentage: 0.9,
                    layout: {
                        padding: {
                            top: 10,
                            bottom: 10,
                            left: 10,
                            right: 10
                        }
                    }
                }
            });
        }
        
        async function updateDashboard() {
            try {
                const statsResponse = await fetch('http://localhost:8001/api/stats');
                const stats = await statsResponse.json();
                
                document.getElementById('totalUsers').textContent = stats.total_users.toLocaleString();
                document.getElementById('totalPoints').textContent = stats.total_reputation_points.toLocaleString();
                document.getElementById('avgScore').textContent = Math.round(stats.average_score);
                
                // Ensure all tier values are numbers, defaulting to 0 if missing
                const tierData = [
                    Number(stats.tier_distribution?.bronze || 0),
                    Number(stats.tier_distribution?.silver || 0),
                    Number(stats.tier_distribution?.gold || 0),
                    Number(stats.tier_distribution?.platinum || 0)
                ];
                
                console.log('Tier Distribution Data:', tierData);
                console.log('Full Stats:', JSON.stringify(stats, null, 2));
                
                // Update chart data - ensure all 4 values are always present
                // Use a small non-zero value (0.1) for zero values to ensure bars are visible
                const displayData = tierData.map(val => val === 0 ? 0.1 : val);
                
                tierChart.data.datasets[0].data = displayData;
                
                // Ensure all labels are present and in correct order
                tierChart.data.labels = ['Bronze', 'Silver', 'Gold', 'Platinum'];
                
                // Log for debugging
                console.log('Updating chart with data:', tierData);
                console.log('Display data (with 0.1 for zeros):', displayData);
                console.log('Chart labels:', tierChart.data.labels);
                
                // Update tooltip to show actual values
                tierChart.options.plugins.tooltip.callbacks.label = function(context) {
                    const actualValue = tierData[context.dataIndex];
                    return 'Users: ' + actualValue;
                };
                
                // Force a complete update
                tierChart.update('none');
                
                // Ensure chart is properly rendered
                requestAnimationFrame(() => {
                    if (tierChart) {
                        tierChart.resize();
                        tierChart.update('none');
                    }
                });
                
                const topResponse = await fetch('http://localhost:8001/api/top-users?limit=10');
                const topUsers = await topResponse.json();
                
                const leaderboard = document.getElementById('leaderboard');
                leaderboard.innerHTML = topUsers.map((user, index) => `
                    <div class="leaderboard-item">
                        <div class="rank">#${index + 1}</div>
                        <div class="user-id">${user.user_id}</div>
                        <div class="tier-badge tier-${user.tier}">${user.tier}</div>
                        <div class="score">${user.score.toLocaleString()}</div>
                    </div>
                `).join('');
                
            } catch (error) {
                console.error('Update failed:', error);
            }
        }
        
        initChart();
        updateDashboard();
        setInterval(updateDashboard, 2000);
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.dashboard_port)

