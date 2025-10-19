FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Generate protobuf code
RUN mkdir -p src/generated && \
    protoc --python_out=src/generated schemas/social_events.proto

EXPOSE 5000

CMD ["python", "src/dashboard.py"]
