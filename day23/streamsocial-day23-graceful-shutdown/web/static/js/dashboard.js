let chart;
let processingData = [];

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeChart();
    startDataUpdates();
    addLogEntry('Dashboard initialized');
});

function initializeChart() {
    const ctx = document.getElementById('processing-chart').getContext('2d');
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Messages/sec',
                data: [],
                borderColor: '#4CAF50',
                backgroundColor: 'rgba(76, 175, 80, 0.1)',
                tension: 0.4,
                fill: true
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Messages per Second'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time'
                    }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            }
        }
    });
}

function startDataUpdates() {
    setInterval(updateDashboard, 2000); // Update every 2 seconds
}

async function updateDashboard() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();
        
        updateSystemStatus(data);
        updateMetrics(data);
        updateShutdownEvents(data);
        updateChart();
        
    } catch (error) {
        console.error('Error updating dashboard:', error);
        updateSystemStatus({health_status: {status: 'error', error: error.message}});
    }
}

function updateSystemStatus(data) {
    const statusElement = document.getElementById('status-text');
    const statusDot = document.querySelector('.status-dot');
    const phaseElement = document.getElementById('shutdown-phase');
    const lastProcessedElement = document.getElementById('last-processed');
    
    if (data.health_status) {
        const status = data.health_status.status || 'unknown';
        statusElement.textContent = status.charAt(0).toUpperCase() + status.slice(1);
        
        // Update status indicator
        statusDot.className = 'status-dot ' + (status === 'healthy' ? 'healthy' : 
                                             status === 'error' ? 'error' : 'warning');
        
        // Update shutdown phase
        const phase = data.health_status.shutdown_phase || 'running';
        phaseElement.textContent = phase.replace('_', ' ').toUpperCase();
        
        // Update last processed
        if (data.health_status.last_processed) {
            const lastProcessed = new Date(data.health_status.last_processed);
            lastProcessedElement.textContent = lastProcessed.toLocaleTimeString();
        }
        
        // Update progress bar based on phase
        const progressBar = document.getElementById('processing-progress');
        const phaseProgress = {
            'running': 100,
            'shutdown_requested': 80,
            'draining': 60,
            'committing': 40,
            'cleanup': 20,
            'terminated': 0
        };
        progressBar.style.width = (phaseProgress[phase] || 100) + '%';
    }
}

function updateMetrics(data) {
    // Simulate some metrics updates
    const messagesCount = document.getElementById('messages-count');
    const avgLatency = document.getElementById('avg-latency');
    const activeConsumers = document.getElementById('active-consumers');
    
    // Generate realistic-looking metrics
    const currentCount = parseInt(messagesCount.textContent) || 0;
    messagesCount.textContent = currentCount + Math.floor(Math.random() * 10);
    
    avgLatency.textContent = (Math.random() * 50 + 10).toFixed(1) + 'ms';
    activeConsumers.textContent = data.health_status?.status === 'healthy' ? '1' : '0';
}

function updateShutdownEvents(data) {
    const timeline = document.getElementById('shutdown-timeline');
    
    if (data.shutdown_events && data.shutdown_events.length > 0) {
        timeline.innerHTML = '';
        data.shutdown_events.reverse().forEach(event => {
            const eventElement = document.createElement('div');
            eventElement.className = 'timeline-event';
            eventElement.innerHTML = `
                <strong>${event.phase.replace('_', ' ').toUpperCase()}</strong><br>
                Consumer: ${event.consumer_id}<br>
                Duration: ${event.duration.toFixed(2)}s<br>
                <small>${new Date(event.timestamp).toLocaleTimeString()}</small>
            `;
            timeline.appendChild(eventElement);
        });
    }
}

function updateChart() {
    const now = new Date();
    const timeLabel = now.toLocaleTimeString();
    const messagesPerSecond = Math.floor(Math.random() * 100 + 50);
    
    // Add new data point
    chart.data.labels.push(timeLabel);
    chart.data.datasets[0].data.push(messagesPerSecond);
    
    // Keep only last 20 data points
    if (chart.data.labels.length > 20) {
        chart.data.labels.shift();
        chart.data.datasets[0].data.shift();
    }
    
    chart.update('none'); // Update without animation for better performance
}

async function simulateShutdown() {
    try {
        const response = await fetch('/api/simulate_shutdown');
        const result = await response.json();
        addLogEntry(`Simulated shutdown: ${result.event.phase}`);
    } catch (error) {
        addLogEntry(`Error simulating shutdown: ${error.message}`, 'error');
    }
}

function clearEvents() {
    document.getElementById('shutdown-timeline').innerHTML = 
        '<div class="timeline-event">No shutdown events</div>';
    addLogEntry('Cleared shutdown events');
}

function addLogEntry(message, type = 'info') {
    const logsContainer = document.getElementById('logs-container');
    const logEntry = document.createElement('div');
    logEntry.className = 'log-entry';
    logEntry.setAttribute('data-time', new Date().toLocaleTimeString());
    logEntry.textContent = message;
    
    logsContainer.appendChild(logEntry);
    
    // Keep only last 50 log entries
    while (logsContainer.children.length > 50) {
        logsContainer.removeChild(logsContainer.firstChild);
    }
    
    // Auto-scroll to bottom
    logsContainer.scrollTop = logsContainer.scrollHeight;
}
