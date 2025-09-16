// StreamSocial Transactional Producers Dashboard JavaScript

class StreamSocialDashboard {
    constructor() {
        this.socket = io();
        this.metrics = {
            totalTransactions: 0,
            successfulTransactions: 0,
            failedTransactions: 0,
            avgLatency: 0
        };
        this.users = [];
        
        this.initializeEventListeners();
        this.initializeSocketEvents();
        this.loadUsers();
        this.startMetricsUpdate();
    }
    
    initializeEventListeners() {
        // Post form submission
        document.getElementById('postForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.createPost();
        });
        
        // Follower sync form submission
        document.getElementById('followerSyncForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.createFollowerSyncPost();
        });
        
        // Character counter for post content
        document.getElementById('content').addEventListener('input', (e) => {
            const charCount = e.target.value.length;
            document.getElementById('charCount').textContent = charCount;
        });
        
        // Metrics refresh button
        document.getElementById('refreshMetrics').addEventListener('click', () => {
            this.refreshMetrics();
        });
        
        // Consume messages button
        document.getElementById('consumeMessages').addEventListener('click', () => {
            this.consumeMessages();
        });
        
        // User selection change for follower sync
        document.getElementById('syncUserId').addEventListener('change', (e) => {
            this.updateFollowersList(e.target.value);
        });
    }
    
    initializeSocketEvents() {
        this.socket.on('connect', () => {
            this.addLogEntry('Connected to StreamSocial API', 'success');
            this.addActivityItem('Connected', 'WebSocket connection established', 'success');
        });
        
        this.socket.on('disconnect', () => {
            this.addLogEntry('Disconnected from StreamSocial API', 'error');
            this.addActivityItem('Disconnected', 'WebSocket connection lost', 'error');
        });
        
        this.socket.on('new_post', (data) => {
            this.addLogEntry(`Post created: ${data.post_id} by user ${data.user_id}`, 'success');
            this.addActivityItem('Post Created', `Atomic operation: ${data.content.substring(0, 50)}...`, 'success');
            this.metrics.totalTransactions++;
            this.metrics.successfulTransactions++;
            this.updateMetricsDisplay();
        });
        
        this.socket.on('follower_sync_post', (data) => {
            this.addLogEntry(`Follower sync: ${data.target_followers.length} timelines updated`, 'success');
            this.addActivityItem('Timeline Sync', `Updated ${data.target_followers.length} follower timelines`, 'success');
            this.metrics.totalTransactions++;
            this.metrics.successfulTransactions++;
            this.updateMetricsDisplay();
        });
    }
    
    async loadUsers() {
        try {
            const response = await fetch('/api/users');
            const result = await response.json();
            
            if (result.success) {
                this.users = result.data;
                this.populateUserSelects();
                this.addLogEntry(`Loaded ${this.users.length} users`, 'info');
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.addLogEntry(`Error loading users: ${error.message}`, 'error');
            this.addActivityItem('Error', 'Failed to load users', 'error');
        }
    }
    
    populateUserSelects() {
        const userSelects = ['userId', 'syncUserId'];
        
        userSelects.forEach(selectId => {
            const select = document.getElementById(selectId);
            select.innerHTML = '<option value="">Choose a user...</option>';
            
            this.users.forEach(user => {
                const option = document.createElement('option');
                option.value = user.user_id;
                option.textContent = `${user.display_name} (@${user.username}) - ${user.followers_count} followers`;
                select.appendChild(option);
            });
        });
    }
    
    updateFollowersList(userId) {
        const followersList = document.getElementById('followersList');
        followersList.innerHTML = '';
        
        if (!userId) return;
        
        // Get potential followers (other users)
        const potentialFollowers = this.users.filter(user => user.user_id !== userId);
        
        potentialFollowers.forEach(user => {
            const checkbox = document.createElement('div');
            checkbox.className = 'follower-checkbox';
            
            const input = document.createElement('input');
            input.type = 'checkbox';
            input.id = `follower_${user.user_id}`;
            input.value = user.user_id;
            input.checked = true; // Auto-select all for demo
            
            const label = document.createElement('label');
            label.htmlFor = input.id;
            label.textContent = user.username;
            
            checkbox.appendChild(input);
            checkbox.appendChild(label);
            followersList.appendChild(checkbox);
        });
    }
    
    async createPost() {
        const userId = document.getElementById('userId').value;
        const content = document.getElementById('content').value;
        const hashtags = document.getElementById('hashtags').value;
        
        if (!userId || !content) {
            alert('Please select a user and enter content');
            return;
        }
        
        const postData = {
            user_id: userId,
            content: content,
            content_type: 'text'
        };
        
        if (hashtags) {
            postData.hashtags = hashtags.split(',').map(tag => tag.trim());
        }
        
        try {
            this.addLogEntry(`Creating post for user ${userId}...`, 'info');
            
            const response = await fetch('/api/posts', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(postData)
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.addLogEntry(`Post created successfully: ${result.post_id}`, 'success');
                document.getElementById('postForm').reset();
                document.getElementById('charCount').textContent = '0';
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.addLogEntry(`Error creating post: ${error.message}`, 'error');
            this.addActivityItem('Error', 'Failed to create post', 'error');
            this.metrics.totalTransactions++;
            this.metrics.failedTransactions++;
            this.updateMetricsDisplay();
        }
    }
    
    async createFollowerSyncPost() {
        const userId = document.getElementById('syncUserId').value;
        const content = document.getElementById('syncContent').value;
        
        if (!userId || !content) {
            alert('Please select a user and enter content');
            return;
        }
        
        // Get selected followers
        const followerCheckboxes = document.querySelectorAll('#followersList input[type="checkbox"]:checked');
        const targetFollowers = Array.from(followerCheckboxes).map(cb => cb.value);
        
        if (targetFollowers.length === 0) {
            alert('Please select at least one follower');
            return;
        }
        
        const syncData = {
            user_id: userId,
            content: content,
            target_followers: targetFollowers
        };
        
        try {
            this.addLogEntry(`Syncing timelines for ${targetFollowers.length} followers...`, 'info');
            
            const response = await fetch('/api/posts/follower-sync', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(syncData)
            });
            
            const result = await response.json();
            
            if (result.success) {
                this.addLogEntry(`Follower timeline sync completed successfully`, 'success');
                document.getElementById('followerSyncForm').reset();
                document.getElementById('followersList').innerHTML = '';
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.addLogEntry(`Error in follower sync: ${error.message}`, 'error');
            this.addActivityItem('Error', 'Failed to sync follower timelines', 'error');
            this.metrics.totalTransactions++;
            this.metrics.failedTransactions++;
            this.updateMetricsDisplay();
        }
    }
    
    async refreshMetrics() {
        try {
            const response = await fetch('/api/metrics');
            const result = await response.json();
            
            if (result.success) {
                this.addLogEntry('Metrics refreshed', 'info');
                // Update additional metrics from server
                this.updateMetricsDisplay();
            }
        } catch (error) {
            this.addLogEntry(`Error refreshing metrics: ${error.message}`, 'error');
        }
    }
    
    async consumeMessages() {
        try {
            const response = await fetch('/api/consume', { method: 'POST' });
            const result = await response.json();
            
            if (result.success) {
                this.addLogEntry(`Consumed ${result.consumed_messages} messages`, 'info');
                this.addActivityItem('Messages Consumed', `Processed ${result.consumed_messages} committed transactions`, 'info');
            }
        } catch (error) {
            this.addLogEntry(`Error consuming messages: ${error.message}`, 'error');
        }
    }
    
    updateMetricsDisplay() {
        document.getElementById('totalTransactions').textContent = this.metrics.totalTransactions;
        
        const successRate = this.metrics.totalTransactions > 0 
            ? Math.round((this.metrics.successfulTransactions / this.metrics.totalTransactions) * 100)
            : 100;
        document.getElementById('successRate').textContent = `${successRate}%`;
        
        // Simulate latency for demo
        const avgLatency = Math.floor(Math.random() * 20) + 15;
        document.getElementById('avgLatency').textContent = `${avgLatency}ms`;
    }
    
    addLogEntry(message, level = 'info') {
        const transactionLog = document.getElementById('transactionLog');
        const timestamp = new Date().toLocaleTimeString();
        
        const logEntry = document.createElement('div');
        logEntry.className = 'log-entry';
        
        logEntry.innerHTML = `
            <span class="log-time">${timestamp}</span>
            <span class="log-level ${level}">${level.toUpperCase()}</span>
            <span class="log-message">${message}</span>
        `;
        
        transactionLog.insertBefore(logEntry, transactionLog.firstChild);
        
        // Keep only last 50 entries
        while (transactionLog.children.length > 50) {
            transactionLog.removeChild(transactionLog.lastChild);
        }
    }
    
    addActivityItem(title, message, type = 'info') {
        const activityFeed = document.getElementById('activityFeed');
        const timestamp = new Date().toLocaleTimeString();
        
        const activityItem = document.createElement('div');
        activityItem.className = `activity-item ${type}`;
        
        activityItem.innerHTML = `
            <span class="timestamp">${timestamp}</span>
            <span class="message"><strong>${title}:</strong> ${message}</span>
        `;
        
        activityFeed.insertBefore(activityItem, activityFeed.firstChild);
        
        // Keep only last 20 entries
        while (activityFeed.children.length > 20) {
            activityFeed.removeChild(activityFeed.lastChild);
        }
    }
    
    startMetricsUpdate() {
        setInterval(() => {
            this.updateMetricsDisplay();
        }, 5000);
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new StreamSocialDashboard();
});
