#!/bin/bash
echo "🚀 Starting StreamSocial Day 1..."

# Activate virtual environment
source venv/bin/activate

# Install frontend dependencies if not exists
if [ ! -d "frontend/node_modules" ]; then
    echo "📦 Installing frontend dependencies..."
    cd frontend && npm install && cd ..
fi

# Build frontend
echo "🏗️ Building frontend..."
cd frontend && npm run build && cd ..

# Create dist directory if it doesn't exist
mkdir -p frontend/dist

# Start backend server
echo "🔧 Starting backend server..."
cd backend && python -m uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!
cd ..

echo "Backend PID: $BACKEND_PID" > .pids

echo "✅ StreamSocial is running!"
echo "🌐 Open http://localhost:8000 in your browser"
echo "📊 API docs: http://localhost:8000/docs"
echo "🛑 Run ./stop.sh to stop the server"

# Wait for user input to keep script running
read -p "Press Enter to stop the server..."
kill $BACKEND_PID 2>/dev/null
