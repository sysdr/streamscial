FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY uploads/ ./uploads/
COPY data/ ./data/
COPY logs/ ./logs/

# Create necessary directories
RUN mkdir -p uploads data logs

# Expose ports
EXPOSE 8080 8081

# Set environment variables
ENV PYTHONPATH=/app/src/main/python

# Default command
CMD ["python", "src/main/python/file_source_connector.py"]
