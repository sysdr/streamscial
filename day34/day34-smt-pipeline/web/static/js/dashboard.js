let updateInterval;

function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    return date.toLocaleTimeString();
}

function updateDashboard() {
    fetch('/api/stats')
        .then(response => response.json())
        .then(data => {
            // Update metrics
            document.getElementById('total-normalized').textContent = data.total_normalized;
            document.getElementById('ios-count').textContent = data.by_source.ios;
            document.getElementById('android-count').textContent = data.by_source.android;
            document.getElementById('web-count').textContent = data.by_source.web;
            
            // Update status
            const statusElement = document.getElementById('pipeline-status');
            statusElement.textContent = data.pipeline_health === 'healthy' ? 'Active' : 'Degraded';
            statusElement.parentElement.style.background = 
                data.pipeline_health === 'healthy' ? '#48bb78' : '#f56565';
            
            // Update action chart
            updateActionChart(data.by_action);
            
            // Update recent events table
            updateEventsTable(data.recent_messages);
        })
        .catch(error => {
            console.error('Error fetching stats:', error);
        });
}

function updateActionChart(actions) {
    const chartDiv = document.getElementById('action-chart');
    const maxCount = Math.max(...Object.values(actions), 1);
    
    chartDiv.innerHTML = '';
    
    for (const [action, count] of Object.entries(actions)) {
        const barContainer = document.createElement('div');
        barContainer.className = 'action-bar';
        
        const label = document.createElement('div');
        label.className = 'action-label';
        label.textContent = action;
        
        const barFill = document.createElement('div');
        barFill.className = 'action-bar-fill';
        barFill.style.width = `${(count / maxCount) * 100}%`;
        barFill.textContent = count;
        
        barContainer.appendChild(label);
        barContainer.appendChild(barFill);
        chartDiv.appendChild(barContainer);
    }
}

function updateEventsTable(messages) {
    const tbody = document.getElementById('events-body');
    
    if (messages.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5">No events yet...</td></tr>';
        return;
    }
    
    tbody.innerHTML = messages.slice(0, 20).map(msg => `
        <tr>
            <td>${formatTimestamp(msg.timestamp)}</td>
            <td><span style="background: #edf2f7; padding: 5px 10px; border-radius: 5px; font-weight: 600;">${msg.source}</span></td>
            <td>${msg.action}</td>
            <td>${msg.user_id}</td>
            <td>${msg.post_id}</td>
        </tr>
    `).join('');
}

// Start updating dashboard
updateInterval = setInterval(updateDashboard, 2000);
updateDashboard(); // Initial update
