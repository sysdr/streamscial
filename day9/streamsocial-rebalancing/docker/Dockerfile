FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY tests/ ./tests/

# Create necessary directories
RUN mkdir -p /tmp

# Set Python path
ENV PYTHONPATH=/app

# Expose ports
EXPOSE 8050 8000

# Command to run the application
CMD ["python", "src/main.py"]
