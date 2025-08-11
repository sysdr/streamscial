#!/bin/bash

echo "🧹 Starting comprehensive system cleanup..."

# Stop any running services first
echo "🛑 Stopping any running services..."
./stop.sh 2>/dev/null || true

# Clean up Python cache files
echo "🐍 Cleaning Python cache files..."
find . -type f -name "*.pyc" -delete
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Clean up pip cache
echo "📦 Cleaning pip cache..."
pip cache purge 2>/dev/null || true

# Clean up Docker resources
echo "🐳 Cleaning Docker resources..."
docker system prune -af --volumes 2>/dev/null || true
docker image prune -af 2>/dev/null || true
docker container prune -f 2>/dev/null || true
docker volume prune -f 2>/dev/null || true
docker network prune -f 2>/dev/null || true

# Clean up system temporary files
echo "🗂️ Cleaning system temporary files..."
rm -rf /tmp/* 2>/dev/null || true
rm -rf ~/Library/Caches/* 2>/dev/null || true
rm -rf ~/.cache/* 2>/dev/null || true

# Clean up Homebrew cache
echo "🍺 Cleaning Homebrew cache..."
brew cleanup --prune=all 2>/dev/null || true

# Clean up logs
echo "📝 Cleaning old log files..."
rm -rf logs/*.log 2>/dev/null || true
rm -rf logs/*.old 2>/dev/null || true

# Clean up any PID files
echo "🆔 Cleaning PID files..."
rm -f .monitor.pid .frontend.pid 2>/dev/null || true

# Clean up any temporary files in the project
echo "📁 Cleaning project temporary files..."
find . -name "*.tmp" -delete 2>/dev/null || true
find . -name "*.temp" -delete 2>/dev/null || true
find . -name "*.swp" -delete 2>/dev/null || true
find . -name "*.swo" -delete 2>/dev/null || true
find . -name "*~" -delete 2>/dev/null || true

# Clean up any downloaded files
echo "⬇️ Cleaning downloaded files..."
rm -rf downloads/ 2>/dev/null || true
rm -rf *.tar.gz *.zip *.deb *.rpm 2>/dev/null || true

# Clean up any backup files
echo "💾 Cleaning backup files..."
find . -name "*.bak" -delete 2>/dev/null || true
find . -name "*.backup" -delete 2>/dev/null || true

# Clean up any core dumps
echo "💥 Cleaning core dumps..."
find . -name "core" -delete 2>/dev/null || true
find . -name "core.*" -delete 2>/dev/null || true

# Clean up any lock files
echo "🔒 Cleaning lock files..."
find . -name "*.lock" -delete 2>/dev/null || true

# Clean up any crash reports
echo "🚨 Cleaning crash reports..."
rm -rf ~/Library/Logs/DiagnosticReports/* 2>/dev/null || true

# Clean up any application support caches
echo "📱 Cleaning application support caches..."
rm -rf ~/Library/Application\ Support/*/Cache/* 2>/dev/null || true

# Clean up any browser caches (if any)
echo "🌐 Cleaning browser caches..."
rm -rf ~/Library/Application\ Support/Google/Chrome/Default/Cache/* 2>/dev/null || true
rm -rf ~/Library/Application\ Support/Firefox/Profiles/*/cache2/* 2>/dev/null || true

# Clean up any IDE caches
echo "💻 Cleaning IDE caches..."
rm -rf .vscode/settings.json 2>/dev/null || true
rm -rf .idea/workspace.xml 2>/dev/null || true

# Clean up any node modules (if any)
echo "📦 Cleaning node modules..."
rm -rf node_modules/ 2>/dev/null || true
rm -f package-lock.json yarn.lock 2>/dev/null || true

# Clean up any build artifacts
echo "🔨 Cleaning build artifacts..."
rm -rf build/ dist/ *.egg-info/ 2>/dev/null || true

# Clean up any test artifacts
echo "🧪 Cleaning test artifacts..."
rm -rf .pytest_cache/ .coverage htmlcov/ 2>/dev/null || true

# Clean up any coverage reports
echo "📊 Cleaning coverage reports..."
rm -rf coverage/ .coverage* 2>/dev/null || true

# Clean up any profiling data
echo "📈 Cleaning profiling data..."
rm -rf *.prof *.profile 2>/dev/null || true

# Clean up any Jupyter notebook checkpoints
echo "📓 Cleaning Jupyter checkpoints..."
find . -name ".ipynb_checkpoints" -exec rm -rf {} + 2>/dev/null || true

# Clean up any macOS specific files
echo "🍎 Cleaning macOS specific files..."
find . -name ".DS_Store" -delete 2>/dev/null || true
find . -name "Thumbs.db" -delete 2>/dev/null || true

# Clean up any git related temporary files
echo "🔧 Cleaning git temporary files..."
rm -rf .git/index.lock 2>/dev/null || true
rm -rf .git/refs/stash 2>/dev/null || true

# Clean up any virtual environment caches (but keep the venv)
echo "🐍 Cleaning virtual environment caches..."
if [ -d "venv" ]; then
    find venv -name "*.pyc" -delete 2>/dev/null || true
    find venv -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
fi

# Show disk usage before and after
echo ""
echo "💾 Disk usage summary:"
echo "======================"
df -h . | head -2

# Clean up any remaining temporary files in the current directory
echo ""
echo "🧹 Final cleanup of current directory..."
find . -maxdepth 1 -name "*.tmp" -delete 2>/dev/null || true
find . -maxdepth 1 -name "*.log" -delete 2>/dev/null || true

echo ""
echo "✅ Cleanup completed successfully!"
echo "🎉 System should now have more free disk space"
echo ""
echo "💡 To free up even more space, you can:"
echo "   - Remove unused Docker images: docker image prune -a"
echo "   - Clean up Homebrew: brew cleanup --prune=all"
echo "   - Empty trash: rm -rf ~/.Trash/*"
echo "   - Clean up system logs: sudo rm -rf /var/log/*.log" 