#!/bin/bash

echo "🧹 StreamSocial Partitioning - Cache & Temp Files Cleanup"
echo "========================================================"

# Clean Python cache files
echo "📝 Cleaning Python cache files..."
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Clean temporary files
echo "🗑️  Cleaning temporary files..."
find . -name "*.log" -delete 2>/dev/null || true
find . -name "*.tmp" -delete 2>/dev/null || true
find . -name "*.temp" -delete 2>/dev/null || true
find . -name ".DS_Store" -delete 2>/dev/null || true
find . -name "*.pid" -delete 2>/dev/null || true
find . -name "*.lock" -delete 2>/dev/null || true
find . -name "*.swp" -delete 2>/dev/null || true
find . -name "*~" -delete 2>/dev/null || true

# Clean Docker cache (optional - uncomment if needed)
# echo "🐳 Cleaning Docker cache..."
# docker system prune -f

# Show cleanup results
echo "✅ Cleanup completed!"
echo "📊 Project directory size:"
du -sh .

# Verify no cache files remain
remaining_files=$(find . -name "*.pyc" -o -name "__pycache__" -o -name "*.log" -o -name "*.tmp" -o -name "*.temp" -o -name ".DS_Store" -o -name "*.pid" -o -name "*.lock" -o -name "*.swp" -o -name "*~" 2>/dev/null | wc -l)

if [ $remaining_files -eq 0 ]; then
    echo "🎉 All cache and temporary files cleaned successfully!"
else
    echo "⚠️  Some files may still remain (check permissions)"
fi 