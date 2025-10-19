class PerformanceDashboard {
    constructor() {
        this.socket = io();
        this.performanceData = [];
        this.maxDataPoints = 50;
        this.setupSocketListeners();
        this.initializeCharts();
    }
    
    setupSocketListeners() {
        this.socket.on('connect', () => {
            console.log('Connected to server');
            this.addLiveFeedItem('🟢 Connected to performance monitoring');
        });
        
        this.socket.on('performance_update', (data) => {
            this.updateMetrics(data);
            this.updateCharts(data);
            this.addLiveFeedItem(`📊 Updated: ${new Date().toLocaleTimeString()}`);
        });
        
        this.socket.on('disconnect', () => {
            console.log('Disconnected from server');
            this.addLiveFeedItem('🔴 Disconnected from server');
        });
    }
    
    updateMetrics(data) {
        const formats = data.formats;
        
        // Update Protocol Buffers metrics
        if (formats.protobuf) {
            document.getElementById('protobuf-size').textContent = 
                `${Math.round(formats.protobuf.payload_size_bytes)} bytes`;
            document.getElementById('protobuf-time').textContent = 
                `${formats.protobuf.total_time_us.toFixed(1)} μs`;
        }
        
        // Update Avro metrics
        if (formats.avro) {
            document.getElementById('avro-size').textContent = 
                `${Math.round(formats.avro.payload_size_bytes)} bytes`;
            document.getElementById('avro-time').textContent = 
                `${formats.avro.total_time_us.toFixed(1)} μs`;
        }
        
        // Update JSON metrics
        if (formats.json) {
            document.getElementById('json-size').textContent = 
                `${Math.round(formats.json.payload_size_bytes)} bytes`;
            document.getElementById('json-time').textContent = 
                `${formats.json.total_time_us.toFixed(1)} μs`;
        }
    }
    
    initializeCharts() {
        // Size comparison chart
        const sizeData = [
            {
                x: ['Protocol Buffers', 'Apache Avro', 'JSON'],
                y: [0, 0, 0],
                type: 'bar',
                marker: {
                    color: ['#e74c3c', '#3498db', '#f39c12']
                }
            }
        ];
        
        const sizeLayout = {
            title: 'Payload Size (bytes)',
            xaxis: { title: 'Serialization Format' },
            yaxis: { title: 'Size (bytes)' },
            margin: { t: 50, l: 50, r: 50, b: 50 }
        };
        
        Plotly.newPlot('size-chart', sizeData, sizeLayout);
        
        // Performance chart
        const perfData = [
            {
                x: ['Protocol Buffers', 'Apache Avro', 'JSON'],
                y: [0, 0, 0],
                type: 'bar',
                marker: {
                    color: ['#e74c3c', '#3498db', '#f39c12']
                }
            }
        ];
        
        const perfLayout = {
            title: 'Total Processing Time (μs)',
            xaxis: { title: 'Serialization Format' },
            yaxis: { title: 'Time (μs)' },
            margin: { t: 50, l: 50, r: 50, b: 50 }
        };
        
        Plotly.newPlot('performance-chart', perfData, perfLayout);
    }
    
    updateCharts(data) {
        const formats = data.formats;
        
        // Update size chart
        if (formats.protobuf && formats.avro && formats.json) {
            const sizeUpdate = {
                y: [[
                    formats.protobuf.payload_size_bytes,
                    formats.avro.payload_size_bytes,
                    formats.json.payload_size_bytes
                ]]
            };
            Plotly.restyle('size-chart', sizeUpdate, [0]);
            
            // Update performance chart
            const perfUpdate = {
                y: [[
                    formats.protobuf.total_time_us,
                    formats.avro.total_time_us,
                    formats.json.total_time_us
                ]]
            };
            Plotly.restyle('performance-chart', perfUpdate, [0]);
        }
        
        // Store data for trends
        this.performanceData.push(data);
        if (this.performanceData.length > this.maxDataPoints) {
            this.performanceData.shift();
        }
    }
    
    addLiveFeedItem(message) {
        const liveFeed = document.getElementById('live-metrics');
        const item = document.createElement('div');
        item.className = 'live-item';
        item.textContent = message;
        
        liveFeed.insertBefore(item, liveFeed.firstChild);
        
        // Keep only last 20 items
        while (liveFeed.children.length > 20) {
            liveFeed.removeChild(liveFeed.lastChild);
        }
    }
}

// Initialize dashboard when page loads
document.addEventListener('DOMContentLoaded', () => {
    new PerformanceDashboard();
});
