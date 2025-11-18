let socket = null;
let statusChart = null;
let metricsHistory = [];

function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    socket = new WebSocket(`${protocol}//${window.location.host}/ws/metrics`);
    
    socket.onopen = () => {
        document.getElementById('connection-status').className = 'status-dot connected';
        document.getElementById('connection-text').textContent = 'Connected';
    };
    
    socket.onclose = () => {
        document.getElementById('connection-status').className = 'status-dot disconnected';
        document.getElementById('connection-text').textContent = 'Disconnected';
        setTimeout(initWebSocket, 5000);
    };
    
    socket.onmessage = (event) => {
        const metrics = JSON.parse(event.data);
        updateDashboard(metrics);
        metricsHistory.push(metrics);
        if (metricsHistory.length > 60) metricsHistory.shift();
        updateChart();
    };
}

function updateDashboard(metrics) {
    // Update counts
    document.getElementById('connector-count').textContent = metrics.total_connectors || 0;
    
    let runningTasks = 0;
    let failedTasks = 0;
    
    metrics.connectors?.forEach(conn => {
        conn.tasks?.forEach(task => {
            if (task.state === 'RUNNING') runningTasks++;
            else if (task.state === 'FAILED') failedTasks++;
        });
    });
    
    document.getElementById('task-count').textContent = runningTasks;
    document.getElementById('failed-count').textContent = failedTasks;
    document.getElementById('last-update').textContent = new Date(metrics.timestamp).toLocaleTimeString();
    
    // Also update the detailed connector stats if they exist
    if (document.getElementById('connector-total-count')) {
        document.getElementById('connector-total-count').textContent = metrics.total_connectors || 0;
        document.getElementById('running-tasks-count').textContent = runningTasks;
        document.getElementById('failed-tasks-count').textContent = failedTasks;
    }
    
    // Update connector list
    const connectorsList = document.getElementById('connectors-list');
    connectorsList.innerHTML = '';
    
    metrics.connectors?.forEach(conn => {
        const card = document.createElement('div');
        card.className = 'connector-card';
        
        const stateClass = conn.state === 'RUNNING' ? 'state-running' : 
                          conn.state === 'FAILED' ? 'state-failed' : 'state-paused';
        
        let tasksHtml = '';
        conn.tasks?.forEach(task => {
            const taskStateClass = task.state === 'RUNNING' ? 'state-running' : 
                                  task.state === 'FAILED' ? 'state-failed' : 'state-paused';
            tasksHtml += `
                <div class="task-item">
                    <span class="task-id">Task ${task.id}</span>
                    <span class="connector-state ${taskStateClass}">${task.state}</span>
                </div>
            `;
        });
        
        card.innerHTML = `
            <div class="connector-header">
                <span class="connector-name">${conn.name}</span>
                <span class="connector-state ${stateClass}">${conn.state}</span>
            </div>
            <div class="task-list">${tasksHtml}</div>
        `;
        
        connectorsList.appendChild(card);
    });
}

function initChart() {
    const ctx = document.getElementById('status-chart').getContext('2d');
    statusChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Running Tasks',
                data: [],
                borderColor: '#10b981',
                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                fill: true,
                tension: 0.4
            }, {
                label: 'Failed Tasks',
                data: [],
                borderColor: '#ef4444',
                backgroundColor: 'rgba(239, 68, 68, 0.1)',
                fill: true,
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255, 255, 255, 0.1)' }
                },
                x: {
                    grid: { display: false }
                }
            },
            plugins: {
                legend: {
                    labels: { color: '#f8fafc' }
                }
            }
        }
    });
}

function updateChart() {
    const labels = metricsHistory.map(m => new Date(m.timestamp).toLocaleTimeString());
    const runningData = metricsHistory.map(m => {
        let running = 0;
        m.connectors?.forEach(c => c.tasks?.forEach(t => {
            if (t.state === 'RUNNING') running++;
        }));
        return running;
    });
    const failedData = metricsHistory.map(m => {
        let failed = 0;
        m.connectors?.forEach(c => c.tasks?.forEach(t => {
            if (t.state === 'FAILED') failed++;
        }));
        return failed;
    });
    
    statusChart.data.labels = labels;
    statusChart.data.datasets[0].data = runningData;
    statusChart.data.datasets[1].data = failedData;
    statusChart.update('none');
}

async function refreshMetrics() {
    const response = await fetch('/api/metrics');
    const metrics = await response.json();
    updateDashboard(metrics);
}

async function restartAllConnectors() {
    const response = await fetch('/api/connectors');
    const connectors = await response.json();
    
    for (const name of Object.keys(connectors)) {
        await fetch(`/api/connector/${name}/restart`, { method: 'POST' });
    }
    
    setTimeout(refreshMetrics, 2000);
}

// Demo data functions
async function loadDemoStats() {
    try {
        const response = await fetch('/api/demo/stats');
        const stats = await response.json();
        
        document.getElementById('demo-user-count').textContent = stats.user_profiles.total;
        document.getElementById('demo-avg-followers').textContent = stats.user_profiles.avg_followers.toLocaleString();
        document.getElementById('demo-avg-engagement').textContent = (stats.user_profiles.avg_engagement_rate * 100).toFixed(2) + '%';
        
        document.getElementById('demo-content-count').textContent = stats.content_metadata.total;
        document.getElementById('demo-avg-views').textContent = stats.content_metadata.avg_views.toLocaleString();
        
        document.getElementById('demo-interaction-count').textContent = stats.user_interactions.total;
    } catch (error) {
        console.error('Error loading demo stats:', error);
    }
}

async function loadDemoUsers(offset = 0) {
    try {
        const response = await fetch(`/api/demo/users?limit=10&offset=${offset}`);
        const data = await response.json();
        const table = document.getElementById('demo-users-table');
        
        if (data.users.length === 0) {
            table.innerHTML = '<p>No users found</p>';
            return;
        }
        
        let html = '<table class="data-table"><thead><tr><th>ID</th><th>Username</th><th>Email</th><th>Followers</th><th>Engagement</th><th>Interests</th></tr></thead><tbody>';
        data.users.forEach(user => {
            html += `
                <tr class="clickable-row" onclick="showUserDetails(${user.user_id})" style="cursor: pointer;">
                    <td>${user.user_id}</td>
                    <td>${user.username}</td>
                    <td>${user.email}</td>
                    <td>${user.follower_count.toLocaleString()}</td>
                    <td>${(user.engagement_rate * 100).toFixed(2)}%</td>
                    <td>${user.interests ? user.interests.join(', ') : 'N/A'}</td>
                </tr>
            `;
        });
        html += '</tbody></table>';
        table.innerHTML = html;
    } catch (error) {
        console.error('Error loading demo users:', error);
        document.getElementById('demo-users-table').innerHTML = '<p>Error loading users</p>';
    }
}

async function loadDemoContent(offset = 0) {
    try {
        const response = await fetch(`/api/demo/content?limit=10&offset=${offset}`);
        const data = await response.json();
        const table = document.getElementById('demo-content-table');
        
        if (data.content.length === 0) {
            table.innerHTML = '<p>No content found</p>';
            return;
        }
        
        let html = '<table class="data-table"><thead><tr><th>ID</th><th>Title</th><th>Category</th><th>Views</th><th>Engagement</th><th>Tags</th></tr></thead><tbody>';
        data.content.forEach(content => {
            html += `
                <tr class="clickable-row" onclick="showContentDetails(${content.content_id})" style="cursor: pointer;">
                    <td>${content.content_id}</td>
                    <td>${content.title || 'N/A'}</td>
                    <td>${content.category || 'N/A'}</td>
                    <td>${content.view_count.toLocaleString()}</td>
                    <td>${content.engagement_score.toFixed(2)}</td>
                    <td>${content.tags ? content.tags.slice(0, 3).join(', ') : 'N/A'}</td>
                </tr>
            `;
        });
        html += '</tbody></table>';
        table.innerHTML = html;
    } catch (error) {
        console.error('Error loading demo content:', error);
        document.getElementById('demo-content-table').innerHTML = '<p>Error loading content</p>';
    }
}

async function loadDemoInteractions(offset = 0, userId = null, contentId = null) {
    try {
        let url = `/api/demo/interactions?limit=10&offset=${offset}`;
        if (userId) url += `&user_id=${userId}`;
        if (contentId) url += `&content_id=${contentId}`;
        
        const response = await fetch(url);
        const data = await response.json();
        const table = document.getElementById('demo-interactions-table');
        
        if (data.interactions.length === 0) {
            table.innerHTML = '<p>No interactions found</p>';
            return;
        }
        
        let html = '<table class="data-table"><thead><tr><th>ID</th><th>User ID</th><th>Content ID</th><th>Type</th><th>Duration</th><th>Created</th></tr></thead><tbody>';
        data.interactions.forEach(interaction => {
            const date = interaction.created_at ? new Date(interaction.created_at).toLocaleString() : 'N/A';
            html += `
                <tr class="clickable-row" onclick="showInteractionDetails(${interaction.interaction_id}, ${interaction.user_id}, ${interaction.content_id})" style="cursor: pointer;">
                    <td>${interaction.interaction_id}</td>
                    <td><span class="link-text" onclick="event.stopPropagation(); showUserDetails(${interaction.user_id})">${interaction.user_id}</span></td>
                    <td><span class="link-text" onclick="event.stopPropagation(); showContentDetails(${interaction.content_id})">${interaction.content_id}</span></td>
                    <td>${interaction.interaction_type}</td>
                    <td>${interaction.duration_seconds}s</td>
                    <td>${date}</td>
                </tr>
            `;
        });
        html += '</tbody></table>';
        table.innerHTML = html;
    } catch (error) {
        console.error('Error loading demo interactions:', error);
        document.getElementById('demo-interactions-table').innerHTML = '<p>Error loading interactions</p>';
    }
}

// Modal functions
function openDetailModal() {
    document.getElementById('detail-modal').style.display = 'block';
}

function closeDetailModal() {
    document.getElementById('detail-modal').style.display = 'none';
    document.getElementById('modal-body').innerHTML = '';
}

// Close modal when clicking outside
window.onclick = function(event) {
    const modal = document.getElementById('detail-modal');
    if (event.target == modal) {
        closeDetailModal();
    }
}

// Detail view functions
async function showUserDetails(userId) {
    try {
        const response = await fetch(`/api/demo/user/${userId}`);
        const data = await response.json();
        
        const modalBody = document.getElementById('modal-body');
        modalBody.innerHTML = `
            <h2>User Details</h2>
            <div class="detail-section">
                <h3>Profile Information</h3>
                <div class="detail-grid">
                    <div class="detail-item">
                        <strong>User ID:</strong> ${data.user.user_id}
                    </div>
                    <div class="detail-item">
                        <strong>Username:</strong> ${data.user.username}
                    </div>
                    <div class="detail-item">
                        <strong>Email:</strong> ${data.user.email}
                    </div>
                    <div class="detail-item">
                        <strong>Followers:</strong> ${data.user.follower_count.toLocaleString()}
                    </div>
                    <div class="detail-item">
                        <strong>Following:</strong> ${data.user.following_count.toLocaleString()}
                    </div>
                    <div class="detail-item">
                        <strong>Account Age:</strong> ${data.user.account_age_days} days
                    </div>
                    <div class="detail-item">
                        <strong>Engagement Rate:</strong> ${(data.user.engagement_rate * 100).toFixed(2)}%
                    </div>
                    <div class="detail-item">
                        <strong>Interests:</strong> ${data.user.interests ? data.user.interests.join(', ') : 'N/A'}
                    </div>
                    <div class="detail-item">
                        <strong>Created:</strong> ${data.user.created_at ? new Date(data.user.created_at).toLocaleString() : 'N/A'}
                    </div>
                </div>
            </div>
            <div class="detail-section">
                <h3>Recent Interactions (${data.recent_interactions.length})</h3>
                <div class="interactions-list">
                    ${data.recent_interactions.length > 0 ? `
                        <table class="data-table">
                            <thead>
                                <tr><th>Content ID</th><th>Type</th><th>Duration</th><th>Date</th></tr>
                            </thead>
                            <tbody>
                                ${data.recent_interactions.map(i => `
                                    <tr onclick="showContentDetails(${i.content_id})" style="cursor: pointer;">
                                        <td>${i.content_id}</td>
                                        <td>${i.interaction_type}</td>
                                        <td>${i.duration_seconds}s</td>
                                        <td>${i.created_at ? new Date(i.created_at).toLocaleString() : 'N/A'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    ` : '<p>No interactions found</p>'}
                </div>
                <button onclick="loadDemoInteractions(0, ${userId}, null)" class="btn btn-primary" style="margin-top: 1rem;">View All Interactions</button>
            </div>
        `;
        openDetailModal();
    } catch (error) {
        console.error('Error loading user details:', error);
        alert('Error loading user details');
    }
}

async function showContentDetails(contentId) {
    try {
        const response = await fetch(`/api/demo/content/${contentId}`);
        const data = await response.json();
        
        const modalBody = document.getElementById('modal-body');
        modalBody.innerHTML = `
            <h2>Content Details</h2>
            <div class="detail-section">
                <h3>Content Information</h3>
                <div class="detail-grid">
                    <div class="detail-item">
                        <strong>Content ID:</strong> ${data.content.content_id}
                    </div>
                    <div class="detail-item">
                        <strong>Creator ID:</strong> <span class="link-text" onclick="showUserDetails(${data.content.creator_id})">${data.content.creator_id}</span>
                    </div>
                    <div class="detail-item">
                        <strong>Title:</strong> ${data.content.title || 'N/A'}
                    </div>
                    <div class="detail-item">
                        <strong>Category:</strong> ${data.content.category || 'N/A'}
                    </div>
                    <div class="detail-item">
                        <strong>Views:</strong> ${data.content.view_count.toLocaleString()}
                    </div>
                    <div class="detail-item">
                        <strong>Engagement Score:</strong> ${data.content.engagement_score.toFixed(2)}
                    </div>
                    <div class="detail-item">
                        <strong>Tags:</strong> ${data.content.tags ? data.content.tags.join(', ') : 'N/A'}
                    </div>
                    <div class="detail-item">
                        <strong>Created:</strong> ${data.content.created_at ? new Date(data.content.created_at).toLocaleString() : 'N/A'}
                    </div>
                </div>
            </div>
            <div class="detail-section">
                <h3>Recent Interactions (${data.recent_interactions.length})</h3>
                <div class="interactions-list">
                    ${data.recent_interactions.length > 0 ? `
                        <table class="data-table">
                            <thead>
                                <tr><th>User ID</th><th>Type</th><th>Duration</th><th>Date</th></tr>
                            </thead>
                            <tbody>
                                ${data.recent_interactions.map(i => `
                                    <tr onclick="showUserDetails(${i.user_id})" style="cursor: pointer;">
                                        <td>${i.user_id}</td>
                                        <td>${i.interaction_type}</td>
                                        <td>${i.duration_seconds}s</td>
                                        <td>${i.created_at ? new Date(i.created_at).toLocaleString() : 'N/A'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    ` : '<p>No interactions found</p>'}
                </div>
                <button onclick="loadDemoInteractions(0, null, ${contentId})" class="btn btn-primary" style="margin-top: 1rem;">View All Interactions</button>
            </div>
        `;
        openDetailModal();
    } catch (error) {
        console.error('Error loading content details:', error);
        alert('Error loading content details');
    }
}

function showInteractionDetails(interactionId, userId, contentId) {
    const modalBody = document.getElementById('modal-body');
    modalBody.innerHTML = `
        <h2>Interaction Details</h2>
        <div class="detail-section">
            <div class="detail-grid">
                <div class="detail-item">
                    <strong>Interaction ID:</strong> ${interactionId}
                </div>
                <div class="detail-item">
                    <strong>User:</strong> <span class="link-text" onclick="showUserDetails(${userId})">View User ${userId}</span>
                </div>
                <div class="detail-item">
                    <strong>Content:</strong> <span class="link-text" onclick="showContentDetails(${contentId})">View Content ${contentId}</span>
                </div>
            </div>
        </div>
    `;
    openDetailModal();
}

// Focus functions for stat cards
function focusOnUsers() {
    document.getElementById('demo-users-table').scrollIntoView({ behavior: 'smooth', block: 'start' });
    // Highlight the users section
    const container = document.querySelector('.demo-table-container');
    if (container) {
        container.style.border = '2px solid var(--primary)';
        setTimeout(() => container.style.border = '', 2000);
    }
}

function focusOnContent() {
    const containers = document.querySelectorAll('.demo-table-container');
    if (containers.length > 1) {
        containers[1].scrollIntoView({ behavior: 'smooth', block: 'start' });
        containers[1].style.border = '2px solid var(--primary)';
        setTimeout(() => containers[1].style.border = '', 2000);
    }
}

function focusOnInteractions() {
    const containers = document.querySelectorAll('.demo-table-container');
    if (containers.length > 2) {
        containers[2].scrollIntoView({ behavior: 'smooth', block: 'start' });
        containers[2].style.border = '2px solid var(--primary)';
        setTimeout(() => containers[2].style.border = '', 2000);
    }
}

function focusOnConnectors() {
    document.getElementById('connectors-demo-table').scrollIntoView({ behavior: 'smooth', block: 'start' });
    const containers = document.querySelectorAll('.connectors-tables-section .demo-table-container');
    if (containers.length > 0) {
        containers[0].style.border = '2px solid var(--primary)';
        setTimeout(() => containers[0].style.border = '', 2000);
    }
}

function focusOnTasks() {
    const containers = document.querySelectorAll('.connectors-tables-section .demo-table-container');
    if (containers.length > 1) {
        containers[1].scrollIntoView({ behavior: 'smooth', block: 'start' });
        containers[1].style.border = '2px solid var(--primary)';
        setTimeout(() => containers[1].style.border = '', 2000);
    }
}

function focusOnFailedTasks() {
    loadAllTasks('FAILED');
    focusOnTasks();
}

// Connectors and Tasks functions
async function loadConnectors() {
    try {
        const response = await fetch('/api/connectors/detailed');
        const data = await response.json();
        
        // Update statistics
        if (data.statistics) {
            document.getElementById('connector-total-count').textContent = data.statistics.total_connectors;
            document.getElementById('running-tasks-count').textContent = data.statistics.running_tasks;
            document.getElementById('failed-tasks-count').textContent = data.statistics.failed_tasks;
            document.getElementById('total-tasks-count').textContent = data.statistics.total_tasks;
        }
        
        // Update connectors table
        const table = document.getElementById('connectors-demo-table');
        
        if (!data.connectors || data.connectors.length === 0) {
            table.innerHTML = '<p>No connectors found. Connectors may not be configured yet.</p>';
            return;
        }
        
        let html = '<table class="data-table"><thead><tr><th>Name</th><th>State</th><th>Class</th><th>Tasks</th><th>Running</th><th>Failed</th><th>Worker ID</th></tr></thead><tbody>';
        data.connectors.forEach(connector => {
            const runningCount = connector.tasks.filter(t => t.state === 'RUNNING').length;
            const failedCount = connector.tasks.filter(t => t.state === 'FAILED').length;
            const stateClass = connector.state === 'RUNNING' ? 'state-running' : 
                             connector.state === 'FAILED' ? 'state-failed' : 'state-paused';
            
            html += `
                <tr class="clickable-row" onclick="showConnectorDetails('${connector.name}')" style="cursor: pointer;">
                    <td><strong>${connector.name}</strong></td>
                    <td><span class="connector-state ${stateClass}">${connector.state}</span></td>
                    <td>${connector.config.connector_class || 'N/A'}</td>
                    <td>${connector.tasks.length}</td>
                    <td>${runningCount}</td>
                    <td>${failedCount > 0 ? `<span class="error">${failedCount}</span>` : '0'}</td>
                    <td>${connector.worker_id || 'N/A'}</td>
                </tr>
            `;
        });
        html += '</tbody></table>';
        table.innerHTML = html;
    } catch (error) {
        console.error('Error loading connectors:', error);
        document.getElementById('connectors-demo-table').innerHTML = '<p>Error loading connectors. Make sure Kafka Connect is running.</p>';
    }
}

async function loadAllTasks(filterState = null) {
    try {
        const response = await fetch('/api/connectors/detailed');
        const data = await response.json();
        
        const table = document.getElementById('tasks-demo-table');
        
        if (!data.connectors || data.connectors.length === 0) {
            table.innerHTML = '<p>No connectors found. No tasks to display.</p>';
            return;
        }
        
        // Collect all tasks from all connectors
        let allTasks = [];
        data.connectors.forEach(connector => {
            connector.tasks.forEach(task => {
                allTasks.push({
                    ...task,
                    connector_name: connector.name,
                    connector_state: connector.state
                });
            });
        });
        
        // Filter by state if specified
        if (filterState) {
            allTasks = allTasks.filter(t => t.state === filterState);
        }
        
        if (allTasks.length === 0) {
            table.innerHTML = `<p>No tasks found${filterState ? ` with state ${filterState}` : ''}.</p>`;
            return;
        }
        
        let html = '<table class="data-table"><thead><tr><th>Connector</th><th>Task ID</th><th>State</th><th>Worker ID</th><th>Error</th></tr></thead><tbody>';
        allTasks.forEach(task => {
            const stateClass = task.state === 'RUNNING' ? 'state-running' : 
                             task.state === 'FAILED' ? 'state-failed' : 'state-paused';
            const errorInfo = task.trace ? task.trace.substring(0, 100) + '...' : 'None';
            
            html += `
                <tr class="clickable-row" onclick="showTaskDetails('${task.connector_name}', ${task.id})" style="cursor: pointer;">
                    <td><span class="link-text" onclick="event.stopPropagation(); showConnectorDetails('${task.connector_name}')">${task.connector_name}</span></td>
                    <td>${task.id}</td>
                    <td><span class="connector-state ${stateClass}">${task.state}</span></td>
                    <td>${task.worker_id || 'N/A'}</td>
                    <td style="max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${task.trace || 'No error'}">${errorInfo}</td>
                </tr>
            `;
        });
        html += '</tbody></table>';
        table.innerHTML = html;
    } catch (error) {
        console.error('Error loading tasks:', error);
        document.getElementById('tasks-demo-table').innerHTML = '<p>Error loading tasks.</p>';
    }
}

async function showConnectorDetails(connectorName) {
    try {
        const response = await fetch(`/api/connector/${connectorName}/detailed`);
        const data = await response.json();
        
        const modalBody = document.getElementById('modal-body');
        modalBody.innerHTML = `
            <h2>Connector Details: ${data.name}</h2>
            <div class="detail-section">
                <h3>Status Information</h3>
                <div class="detail-grid">
                    <div class="detail-item">
                        <strong>Name:</strong> ${data.name}
                    </div>
                    <div class="detail-item">
                        <strong>State:</strong> <span class="connector-state ${data.state === 'RUNNING' ? 'state-running' : data.state === 'FAILED' ? 'state-failed' : 'state-paused'}">${data.state}</span>
                    </div>
                    <div class="detail-item">
                        <strong>Worker ID:</strong> ${data.worker_id || 'N/A'}
                    </div>
                </div>
            </div>
            <div class="detail-section">
                <h3>Configuration</h3>
                <div class="detail-grid">
                    ${Object.entries(data.config || {}).slice(0, 10).map(([key, value]) => `
                        <div class="detail-item">
                            <strong>${key}:</strong> ${typeof value === 'string' && value.length > 100 ? value.substring(0, 100) + '...' : value}
                        </div>
                    `).join('')}
                </div>
            </div>
            <div class="detail-section">
                <h3>Tasks (${data.tasks.length})</h3>
                <div class="interactions-list">
                    ${data.tasks.length > 0 ? `
                        <table class="data-table">
                            <thead>
                                <tr><th>Task ID</th><th>State</th><th>Worker ID</th><th>Error</th></tr>
                            </thead>
                            <tbody>
                                ${data.tasks.map(t => `
                                    <tr onclick="showTaskDetails('${data.name}', ${t.id})" style="cursor: pointer;">
                                        <td>${t.id}</td>
                                        <td><span class="connector-state ${t.state === 'RUNNING' ? 'state-running' : t.state === 'FAILED' ? 'state-failed' : 'state-paused'}">${t.state}</span></td>
                                        <td>${t.worker_id || 'N/A'}</td>
                                        <td style="max-width: 200px; overflow: hidden; text-overflow: ellipsis;">${t.trace ? t.trace.substring(0, 50) + '...' : 'None'}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    ` : '<p>No tasks found</p>'}
                </div>
            </div>
        `;
        openDetailModal();
    } catch (error) {
        console.error('Error loading connector details:', error);
        alert('Error loading connector details');
    }
}

function showTaskDetails(connectorName, taskId) {
    // For now, just show connector details and highlight the task
    showConnectorDetails(connectorName);
}

document.addEventListener('DOMContentLoaded', () => {
    initChart();
    initWebSocket();
    loadDemoStats();
    loadDemoUsers(0);
    loadDemoContent(0);
    loadDemoInteractions(0);
    loadConnectors();
    loadAllTasks();
    
    // Refresh connectors and tasks every 10 seconds
    setInterval(() => {
        loadConnectors();
        loadAllTasks();
    }, 10000);
});
