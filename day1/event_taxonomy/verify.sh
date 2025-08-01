#!/bin/bash
echo "🔍 Verifying StreamSocial setup..."

# Check Python version
python --version | grep "3.11" > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ Python 3.11 detected"
else
    echo "❌ Python 3.11 not found"
fi

# Check virtual environment
if [ -d "venv" ]; then
    echo "✅ Virtual environment created"
else
    echo "❌ Virtual environment missing"
fi

# Check backend files
BACKEND_FILES=(
    "backend/src/main.py"
    "backend/src/events/models.py"
    "backend/src/events/event_bus.py"
    "backend/src/handlers/feed_handler.py"
    "backend/src/handlers/notification_handler.py"
)

for file in "${BACKEND_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

# Check frontend files
FRONTEND_FILES=(
    "frontend/src/App.jsx"
    "frontend/src/components/EventDashboard.jsx"
    "frontend/src/components/EventPublisher.jsx"
    "frontend/package.json"
)

for file in "${FRONTEND_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ $file exists"
    else
        echo "❌ $file missing"
    fi
done

# Check if frontend is built
if [ -d "frontend/dist" ]; then
    echo "✅ Frontend built"
else
    echo "❌ Frontend not built"
fi

echo "🎯 Setup verification complete!"
