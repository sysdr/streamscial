#!/bin/bash

# StreamSocial Timeline Ordering Start Script
set -e

function start_kafka() {
    echo "🚀 Starting Kafka infrastructure..."
    cd docker
    docker-compose up -d zookeeper kafka kafka-ui
    cd ..
    
    echo "⏳ Waiting for Kafka to be ready..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if docker-compose -f docker/docker-compose.yml exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
            echo "✅ Kafka is ready!"
            break
        fi
        echo "   Waiting for Kafka... ($timeout seconds remaining)"
        sleep 2
        timeout=$((timeout-2))
    done
    
    if [ $timeout -le 0 ]; then
        echo "❌ Kafka failed to start in time"
        exit 1
    fi
    
    # Create topic
    echo "📝 Creating topic 'user-timeline'..."
    docker-compose -f docker/docker-compose.yml exec -T kafka kafka-topics --create \
        --topic user-timeline \
        --bootstrap-server localhost:9092 \
        --partitions 6 \
        --replication-factor 1 \
        --if-not-exists || true
}

function start_app() {
    echo "🌐 Starting StreamSocial Timeline Application..."
    
    # Activate virtual environment
    source venv/bin/activate
    export PYTHONPATH="${PWD}:${PYTHONPATH}"
    
    # Start the web application
    echo "🚀 Starting web dashboard on http://localhost:8000"
    python -m uvicorn src.web.dashboard:app --host 0.0.0.0 --port 8000 --reload
}

function show_demo_instructions() {
    echo ""
    echo "🎉 StreamSocial Timeline Ordering System Started!"
    echo "================================================"
    echo ""
    echo "🌐 Web Dashboard: http://localhost:8000"
    echo "🔧 Kafka UI: http://localhost:8080"
    echo ""
    echo "📋 Demo Instructions:"
    echo "1. Open the web dashboard in your browser"
    echo "2. Send test messages using different user IDs"
    echo "3. Check user timelines to verify chronological ordering"
    echo "4. Analyze partition distribution"
    echo "5. Verify ordering consistency"
    echo ""
    echo "🧪 Try these test scenarios:"
    echo "- Send multiple messages from the same user (alice, bob, etc.)"
    echo "- Check that messages appear in chronological order"
    echo "- Analyze how users are distributed across partitions"
    echo "- Observe live updates in the dashboard"
    echo ""
    echo "🛑 To stop: Run './stop.sh' or press Ctrl+C"
}

# Parse command line arguments
case "${1:-all}" in
    "kafka")
        start_kafka
        ;;
    "app")
        start_app
        ;;
    "all"|"")
        start_kafka
        echo ""
        show_demo_instructions
        echo ""
        echo "⏳ Starting application in 3 seconds..."
        sleep 3
        start_app
        ;;
    *)
        echo "Usage: $0 [kafka|app|all]"
        echo "  kafka - Start only Kafka infrastructure"
        echo "  app   - Start only the StreamSocial application"
        echo "  all   - Start everything (default)"
        exit 1
        ;;
esac
