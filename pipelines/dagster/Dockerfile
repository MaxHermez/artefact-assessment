FROM python:3.11-slim

# Set working directory
WORKDIR /opt/dagster/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create Dagster home directory and copy configuration
RUN mkdir -p /opt/dagster/dagster_home && \
    cp /opt/dagster/app/dagster.yaml /opt/dagster/dagster_home/dagster.yaml

# Set environment variables
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Expose Dagster webserver port
EXPOSE 3000

# Default command (can be overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
