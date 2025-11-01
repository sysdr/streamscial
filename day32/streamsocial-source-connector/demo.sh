#!/bin/bash

echo "=========================================="
echo "StreamSocial Source Connector Demo"
echo "=========================================="

# Create sample files for processing
echo "Creating sample upload files..."
mkdir -p uploads/user123
mkdir -p uploads/user456

# Create sample files
echo "Sample image content for user 123" > uploads/user123/profile_pic.jpg
echo "Sample document content" > uploads/user123/document.pdf
echo "Another user's content" > uploads/user456/avatar.png

# Show file structure
echo "Upload directory structure:"
find uploads -type f

# Monitor logs
echo "Monitoring connector logs (press Ctrl+C to stop)..."
tail -f logs/connector.log &
LOG_PID=$!

# Wait a bit
sleep 5

# Add more files to trigger processing
echo "Adding more files to trigger processing..."
echo "New content $(date)" > uploads/user123/new_file_$(date +%s).txt
echo "Video content placeholder" > uploads/user456/video.mp4

# Show offset file
echo "Current offset status:"
if [ -f data/offsets.json ]; then
    cat data/offsets.json | python -m json.tool
fi

# Clean up
sleep 10
kill $LOG_PID

echo "Demo completed!"
